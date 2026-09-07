//
// Copyright 2026 Tabsdata Inc.
//

//! `function_versions__with_names` — the one surface where head versions
//! and patches appear together, for version-history listings and
//! fvid-addressed reads. Patch rows borrow the identity columns they
//! don't own (name, decorator, storage fields, ...) from their parent
//! head version and expose `patch_of`; head rows have `patch_of` NULL.
//! A patch's `status` is effective: the parent's when the parent is a
//! deleted tombstone, its own otherwise — so a deleted function hides
//! its patches exactly as it hides its own versions. Every other query
//! path stays patch-blind by construction.

use sea_query::{Alias, Asterisk, Expr, ExprTrait, Iden, JoinType, Query, UnionType};

use super::collections::{Collections, CollectionsResolved};
use super::function_patches::FunctionPatches;
use super::functions::Functions;
use super::users::Users;
use crate::schema::Entity;
use crate::schema::render::name_or_id;
use crate::schema::sql::ViewDef;

#[derive(Iden, Clone, Copy)]
#[iden = "function_versions__with_names"]
pub struct FunctionVersionsWithNames;

pub(super) struct Defs;

impl Entity for Defs {
    fn create_views(&self) -> Vec<ViewDef> {
        let parent = Alias::new("parent");
        let mut heads = Query::select()
            .expr(Expr::col((Functions::Table, Asterisk)))
            .expr_as(Expr::col((CollectionsResolved, Collections::Name)), Alias::new("collection"))
            .expr_as(name_or_id((Users::Table, Users::Name), (Functions::Table, Functions::DefinedById)), Alias::new("defined_by"))
            .expr_as(Expr::val(None::<String>), Alias::new("patch_of"))
            .from(Functions::Table)
            .join(
                JoinType::LeftJoin,
                CollectionsResolved,
                Expr::col((Functions::Table, Functions::CollectionId)).equals((CollectionsResolved, Collections::Id)),
            )
            .join(JoinType::LeftJoin, Users::Table, Expr::col((Functions::Table, Functions::DefinedById)).equals((Users::Table, Users::Id)))
            .to_owned();
        let patches = Query::select()
            .expr(Expr::col((FunctionPatches::Table, FunctionPatches::Id)))
            .expr(Expr::col((FunctionPatches::Table, FunctionPatches::ProjectId)))
            .expr(Expr::col((FunctionPatches::Table, FunctionPatches::CollectionId)))
            .expr(Expr::col((parent.clone(), Functions::Name)))
            .expr(Expr::col((FunctionPatches::Table, FunctionPatches::Description)))
            .expr(Expr::col((parent.clone(), Functions::Decorator)))
            .expr(Expr::col((FunctionPatches::Table, FunctionPatches::RuntimeValues)))
            .expr(Expr::col((FunctionPatches::Table, FunctionPatches::FunctionId)))
            .expr(Expr::col((parent.clone(), Functions::BasePath)))
            .expr(Expr::col((parent.clone(), Functions::StageId)))
            .expr(Expr::col((parent.clone(), Functions::StageBasePath)))
            .expr(Expr::col((parent.clone(), Functions::StorageVersion)))
            .expr(Expr::col((FunctionPatches::Table, FunctionPatches::BundleId)))
            .expr(Expr::col((FunctionPatches::Table, FunctionPatches::Snippet)))
            .expr(Expr::col((FunctionPatches::Table, FunctionPatches::DefinedOn)))
            .expr(Expr::col((FunctionPatches::Table, FunctionPatches::DefinedById)))
            .expr(Expr::col((FunctionPatches::Table, FunctionPatches::Status)))
            .expr(Expr::col((parent.clone(), Functions::Connector)))
            .expr(Expr::col((parent.clone(), Functions::Connection)))
            .expr(Expr::col((parent.clone(), Functions::Enabled)))
            .expr(Expr::col((FunctionPatches::Table, FunctionPatches::ValidTo)))
            .expr(Expr::col((CollectionsResolved, Collections::Name)))
            .expr(name_or_id((Users::Table, Users::Name), (FunctionPatches::Table, FunctionPatches::DefinedById)))
            .expr(Expr::col((FunctionPatches::Table, FunctionPatches::ParentFunctionVersionId)))
            .from(FunctionPatches::Table)
            .join_as(
                JoinType::InnerJoin,
                Functions::Table,
                parent.clone(),
                Expr::col((FunctionPatches::Table, FunctionPatches::ParentFunctionVersionId)).equals((parent, Functions::Id)),
            )
            .join(
                JoinType::LeftJoin,
                CollectionsResolved,
                Expr::col((FunctionPatches::Table, FunctionPatches::CollectionId)).equals((CollectionsResolved, Collections::Id)),
            )
            .join(
                JoinType::LeftJoin,
                Users::Table,
                Expr::col((FunctionPatches::Table, FunctionPatches::DefinedById)).equals((Users::Table, Users::Id)),
            )
            .to_owned();
        vec![ViewDef::new(
            FunctionVersionsWithNames,
            heads.union(UnionType::All, patches).to_owned(),
        )]
    }
}
