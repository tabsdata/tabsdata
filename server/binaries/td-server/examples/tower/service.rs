//
//  Copyright 2024 Tabs Data Inc.
//

//! This module contains the service provider pattern implementation.
//! It showcases how to build modular services that can be reused in different contexts.

use std::ops::Deref;
use std::sync::Arc;
use td_tower::default_services::{ServiceEntry, ServiceReturn, Share, SrvCtxProvider};
use td_tower::error::FromHandlerError;
use td_tower::extractors::{Input, SrvCtx};
use td_tower::from_fn::from_fn;
use td_tower::service_provider::{IntoServiceProvider, ServiceProvider, TdBoxService};
use tower::{ServiceBuilder, ServiceExt};

/// In this example, we have a NameChecker, composed of 3 services:
/// - Format Checker: checks if the name is the expected one and if the first letter is a capital letter.
/// - Shortener: shortens the name to a maximum length.
/// - Format Checker and Shortener: combines functionality of the previous two services.
///
/// This showcases how to build modular services that can be reused in different contexts, by
/// coding independent data. These data can be completely independent and then combined
/// on services that require them.
/// It is also showcased how types can get through the service chain, by using the ContextProvider
/// and Input types (if needed, other Extractors can be used or added, like Connection for sql connections).
/// Note that a single type of each will be passed through the chain, so if more than one is needed,
/// use wrappers of those types (i.e. NewType(Type)).

#[tokio::main]
async fn main() {
    let name_creator = NameChecker::new("JoaquinBo");

    let service = name_creator.format_checker().await;
    let response = service.oneshot(String::from("JoaquinBo")).await.unwrap();
    assert_eq!(*response, "JoaquinBo");

    let service = name_creator.format_checker().await;
    let response = service.oneshot(String::from("NotJoaquin")).await;
    assert!(matches!(response, Err(NameCreatorError::InvalidName(_))));

    let service = name_creator.shortener().await;
    let response = service
        .oneshot(String::from("JoaquinButLong"))
        .await
        .unwrap();
    assert_eq!(*response, "JoaquinB");

    let service = name_creator.format_checker_and_shortener().await;
    let response = service.oneshot(String::from("JoaquinBo")).await.unwrap();
    assert_eq!(*response, "JoaquinB");

    let name_creator = NameChecker::new("joaquin");

    let service = name_creator.format_checker().await;
    let response = service.oneshot(String::from("joaquin")).await;
    assert!(matches!(response, Err(NameCreatorError::CapitalLetter)));
}

#[derive(Debug, thiserror::Error)]
enum NameCreatorError {
    #[error("Invalid name {0}")]
    InvalidName(String),
    #[error("First letter is not a capital letter")]
    CapitalLetter,
    #[error("Service Handler Error: {0}")]
    ServiceHandlerError(#[from] FromHandlerError),
}

struct NameChecker {
    format_checker_provider: ServiceProvider<String, String, NameCreatorError>,
    shortener_provider: ServiceProvider<String, String, NameCreatorError>,
    format_checker_and_shortener_provider: ServiceProvider<String, String, NameCreatorError>,
}

impl NameChecker {
    pub fn new(expected_name: &str) -> Self {
        NameChecker {
            format_checker_provider: Self::format_checker_provider(expected_name.to_string()),
            shortener_provider: Self::shortener_provider(),
            format_checker_and_shortener_provider: Self::format_checker_and_shortener_provider(
                expected_name.to_string(),
            ),
        }
    }

    fn format_checker_provider<Req: Share, Res: Share>(
        expected_name: String,
    ) -> ServiceProvider<Req, Res, NameCreatorError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(SrvCtxProvider::new(Arc::new(expected_name)))
            .layer(from_fn(expected_name_checker))
            .layer(from_fn(capital_letter_checker))
            .service(ServiceReturn)
            .into_service_provider()
    }

    async fn format_checker(&self) -> TdBoxService<String, String, NameCreatorError> {
        self.format_checker_provider.make().await
    }

    fn shortener_provider<Req: Share, Res: Share>() -> ServiceProvider<Req, Res, NameCreatorError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(SrvCtxProvider::new(Arc::new(8)))
            .layer(from_fn(name_shortener))
            .service(ServiceReturn)
            .into_service_provider()
    }

    async fn shortener(&self) -> TdBoxService<String, String, NameCreatorError> {
        self.shortener_provider.make().await
    }

    fn format_checker_and_shortener_provider<Req: Share, Res: Share>(
        expected_name: String,
    ) -> ServiceProvider<Req, Res, NameCreatorError> {
        ServiceBuilder::new()
            .layer(ServiceEntry::default())
            .layer(SrvCtxProvider::new(Arc::new(expected_name)))
            .layer(SrvCtxProvider::new(Arc::new(8)))
            .layer(from_fn(expected_name_checker))
            .layer(from_fn(capital_letter_checker))
            .layer(from_fn(name_shortener))
            .service(ServiceReturn)
            .into_service_provider()
    }

    async fn format_checker_and_shortener(&self) -> TdBoxService<String, String, NameCreatorError> {
        self.format_checker_and_shortener_provider.make().await
    }
}

async fn expected_name_checker(
    Input(name): Input<String>,
    SrvCtx(expected_name): SrvCtx<String>,
) -> Result<(), NameCreatorError> {
    if name == expected_name {
        Ok(())
    } else {
        Err(NameCreatorError::InvalidName(name.deref().clone()))
    }
}

async fn capital_letter_checker(Input(name): Input<String>) -> Result<(), NameCreatorError> {
    match name.chars().next() {
        Some(c) if c.is_uppercase() => Ok(()),
        _ => Err(NameCreatorError::CapitalLetter),
    }
}

async fn name_shortener(
    Input(name): Input<String>,
    SrvCtx(max_length): SrvCtx<i32>,
) -> Result<String, NameCreatorError> {
    let new_name = name.chars().take(*max_length as usize).collect::<String>();
    Ok(new_name)
}
