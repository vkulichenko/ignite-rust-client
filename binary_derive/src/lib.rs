extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn;
use syn::{Data, Fields};

#[proc_macro_derive(IgniteRead)]
pub fn binary_read_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();

    impl_binary_read(&ast)
}

fn impl_binary_read(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;

    let mut field_names = Vec::new();

    match &ast.data {
        Data::Struct(data) => {
            match &data.fields {
                Fields::Named(fields) => {
                    for field in &fields.named {
                        field_names.push(field.clone().ident.unwrap());
                    }
                },
                _ => panic!("Only named fields are supported."),
            }
        },
        _ => panic!("Only struct is supported."),
    };

    let gen = quote! {
        impl IgniteRead for #name {
            fn read(bytes: &mut Bytes) -> Result<#name> {
                Ok(#name {
                    #( #field_names: IgniteRead::read(bytes)?, )*
                })
            }
        }
    };

    gen.into()
}
