extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn;
use syn::{Data, Fields};

#[proc_macro_derive(IgniteRead)]
pub fn binary_read_derive(input: TokenStream) -> TokenStream {
    let ast: syn::DeriveInput = syn::parse(input).unwrap();

    let name = &ast.ident;

    let gen = match &ast.data {
        Data::Struct(data) => {
            let mut field_names = Vec::new();

            match &data.fields {
                Fields::Named(fields) => {
                    for field in &fields.named {
                        field_names.push(field.clone().ident.unwrap());
                    }
                },
                _ => panic!("Only named fields are supported."),
            }

            quote! {
                impl IgniteRead for #name {
                    fn read(bytes: &mut Bytes) -> Result<#name> {
                        Ok(#name {
                            #( #field_names: IgniteRead::read(bytes)?, )*
                        })
                    }
                }
            }
        },
        Data::Enum(_) => {
            quote! {
                impl IgniteRead for #name {
                    fn read(bytes: &mut Bytes) -> Result<#name> {
                        let value: Option<#name> = FromPrimitive::from_i32(i32::read(bytes)?);

                        match value {
                            Some(value) => Ok(value),
                            None => Err(Error::new(ErrorKind::Serde, format!("Failed to read enum: {}", type_name::<#name>()))),
                        }
                    }
                }
            }
        },
        Data::Union(_) => panic!("Union not supported."),
    };

    gen.into()
}

#[proc_macro_derive(IgniteWrite)]
pub fn binary_write_derive(input: TokenStream) -> TokenStream {
    let ast: syn::DeriveInput = syn::parse(input).unwrap();

    let name = &ast.ident;

    let gen = match &ast.data {
        Data::Struct(data) => {
            let mut field_names = Vec::new();

            match &data.fields {
                Fields::Named(fields) => {
                    for field in &fields.named {
                        field_names.push(field.clone().ident.unwrap());
                    }
                },
                _ => panic!("Only named fields are supported."),
            }

            quote! {
                impl IgniteWrite for #name {
                    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
                        #( self.#field_names.write(bytes)?; )*

                        Ok(())
                    }
                }
            }
        },
        Data::Enum(_) => {
            quote! {
                impl IgniteWrite for #name {
                    fn write(&self, bytes: &mut BytesMut) -> Result<()> {
                        let value = self.to_i32().ok_or_else(|| Error::new(ErrorKind::Serde, format!("Failed to write enum: {}", type_name::<#name>())))?;

                        value.write(bytes)
                    }
                }
            }
        },
        Data::Union(_) => panic!("Union not supported."),
    };

    gen.into()
}
