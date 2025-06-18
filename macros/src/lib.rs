//! macros
#![warn(elided_lifetimes_in_paths)]
#![warn(missing_docs)]
#![warn(unreachable_pub)]
#![warn(unused_crate_dependencies)]
#![warn(unused_import_braces)]
#![warn(unused_lifetimes)]
#![warn(unused_qualifications)]
#![deny(unsafe_code)]
#![deny(unsafe_op_in_unsafe_fn)]
#![deny(unused_results)]
#![deny(missing_debug_implementations)]
#![deny(missing_copy_implementations)]
#![warn(clippy::pedantic)]
#![allow(clippy::doc_markdown)]
#![allow(clippy::let_underscore_untyped)]
#![allow(clippy::similar_names)]

use proc::{
    quote::{ToTokens, format_ident, quote},
    syn::{self, Error},
};

/// .
#[proc::attribute]
pub fn spawn(item: syn::ItemFn) -> proc::Result<proc::TokenStream> {
    check_fn(&item)?;

    let vis = &item.vis;
    let mut have_self = false;
    let args = &item
        .sig
        .inputs
        .iter()
        .map(|arg| match arg {
            syn::FnArg::Receiver(_) => {
                have_self = true;
                Ok(quote!(self))
            }
            syn::FnArg::Typed(pat_type) => name(pat_type),
        })
        .collect::<proc::Result<Vec<_>>>()?;

    // Top level function is async & returns ()
    let top_sig = {
        let mut sig = item.sig.clone();
        sig.output = syn::ReturnType::Default;
        sig
    };

    // Inside that is a _non_ async function which spawns the original function
    // on the executor
    let spawner = {
        let mut sig = item.sig.clone();
        sig.output = syn::ReturnType::Default;
        sig.asyncness = None;
        sig.ident = format_ident!("__{}_spawner", item.sig.ident);
        sig
    };
    let spawner_ident = &spawner.ident;
    let spawner_ident = if have_self {
        quote!(Self::#spawner_ident)
    } else {
        spawner_ident.to_token_stream()
    };

    // The original function needs a new name & we'll make it private
    let inner = {
        let mut inner = item.clone();
        inner.sig.ident = format_ident!("__{}_original", item.sig.ident);
        inner.vis = syn::Visibility::Inherited;
        inner
    };
    let inner_ident = &inner.sig.ident;
    let inner_ident = if have_self {
        quote!(Self::#inner_ident)
    } else {
        inner_ident.to_token_stream()
    };

    Ok(quote! {
        #[allow(clippy::unusued_async)]
        #vis #top_sig {
            #spawner_ident(#(#args),*)
        }

        #spawner {
            drop(::tokio::spawn(async move { #inner_ident(#(#args),*).await }))
        }

        #inner
    })
}

fn name(pat_type: &syn::PatType) -> proc::Result<proc::TokenStream> {
    match &*pat_type.pat {
        syn::Pat::Ident(pat_ident) => Ok(pat_ident.ident.to_token_stream()),
        _ => todo!(),
    }
}

fn check_fn(item: &syn::ItemFn) -> proc::Result<()> {
    if item.sig.asyncness.is_none() {
        return Err(Error::new_spanned(
            &item.sig.ident,
            "#[spawn] functions should be async",
        ));
    }

    match &item.sig.output {
        syn::ReturnType::Default => (),
        syn::ReturnType::Type(_, typ) => match &**typ {
            syn::Type::Never(_) => (),
            syn::Type::Tuple(typ) if typ.elems.is_empty() => (),
            _ => {
                return Err(Error::new_spanned(
                    &item.sig.output,
                    "#[spawn] functions should return () or !",
                ));
            }
        },
    }

    Ok(())
}
