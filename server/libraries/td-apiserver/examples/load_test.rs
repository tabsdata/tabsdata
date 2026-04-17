//
// Copyright 2026 Tabs Data Inc.
//

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use rand::RngExt;
use rand::prelude::{IndexedRandom, IteratorRandom};
use std::collections::HashMap;
use std::time::Instant;
use strum::IntoEnumIterator;
use td_common::logging;
use td_common::logging::CONSOLE_PROFILE;
use td_database::config::DatabaseConfig;
use td_execution::TransactionBy;
use td_objects::Buildable;
use td_objects::dxo::function::{FunctionDB, FunctionRegister};
use td_objects::dxo::graph::FunctionNode;
use td_objects::dxo::table::{TableDB, TableDBWithNames};
use td_objects::transaction::map::TransactionMapper;
use td_objects::types::basic::{
    BundleId, CollectionName, Decorator, FunctionName, FunctionRunStatus, TableNameDto,
    UserEnabled, UserName,
};
use td_objects::types::composed::{TableDependencyDto, TableTriggerDto};
use td_queries::BasicQueries;
use td_queries::test_utils::SeedQueries;
use tracing::Level;
use tracing::{debug, info};
use url::Url;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Number of users to create
    #[arg(long, default_value_t = 3)]
    users: usize,

    /// Number of collections.
    #[arg(long, default_value_t = 5)]
    collections: usize,

    /// Number of functions to be randomly split into collections.
    #[arg(long, default_value_t = 5)]
    functions: usize,

    /// Number of tables to be randomly split into functions.
    #[arg(long, default_value_t = 3)]
    tables: usize,

    /// Max number of dependencies per function.
    #[arg(long, default_value_t = 3)]
    dependencies: usize,

    /// Max number of triggers per function.
    #[arg(long, default_value_t = 2)]
    triggers: usize,

    /// Max number of executions per function.
    #[arg(long, default_value_t = 5)]
    executions: usize,

    /// Max number of transactions per execution.
    #[arg(long, default_value_t = 5)]
    transactions: usize,

    /// Database URL (defaults to env var DATABASE_URL or specific file)
    #[arg(long)]
    db_url: Option<Url>,
}

/// Automated script to bulk load a tabsdata database. The created database is used to test
/// the system with a high volume of data. Note that the created plans and versions do not
/// create any real data, just database mocks. They are meant to check queries and other
/// components behave properly. Useful to pair this with database tracing.
/// The created database can be replaced in any real tabsdata instance, and then started.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    logging::start(
        Some(Level::INFO),
        Some(CONSOLE_PROFILE.to_string()),
        false,
        true,
    );

    // let args = Args::parse();
    let args = Args {
        users: 100,
        collections: 1_000,
        functions: 10_000,
        tables: 20_000,
        dependencies: 20_000,
        triggers: 20_000,
        executions: 1_000_000,
        transactions: 5,
        db_url: None,
    };
    info!("Starting load test with config: {:?}", args);

    let db_url = args.db_url.unwrap_or_else(|| {
        Url::from_file_path(td_common::tdtestdir!().join("tabsdata.db")).unwrap()
    });
    let database_config = DatabaseConfig::new(db_url).unwrap();
    info!("Connecting to database with: {database_config:?}");
    let db = td_database::db(&database_config).await?;
    db.upgrade().await?;

    let start_total = Instant::now();

    let pb_style = ProgressStyle::default_bar()
        .template(
            "{spinner:.green} {msg} [{elapsed_precise}] [{bar:40.cyan/blue}] {pos}/{len} ({eta})",
        )?
        .progress_chars("#>-");

    // 1. Create Users
    let mut users = Vec::new();
    let start = Instant::now();
    let pb = ProgressBar::new(args.users as u64);
    pb.set_style(pb_style.clone());
    pb.set_message("Creating users...");
    for i in 0..args.users {
        let name = UserName::try_from(format!("load_user_{i}"))?;
        let user = db.seed_user(&name, &UserEnabled::TRUE).await?;
        users.push(user);
        pb.inc(1);
    }
    pb.finish_with_message("Created users");
    info!("Created {} users in {:.2?}", args.users, start.elapsed());

    // 2. Create Collections with randomized users
    let mut collections = HashMap::new();
    let start = Instant::now();
    let pb = ProgressBar::new(args.collections as u64);
    pb.set_style(pb_style.clone());
    pb.set_message("Creating collections...");
    for i in 0..args.collections {
        let col_name = CollectionName::try_from(format!("load_col_{i}"))?;
        let user = users.choose(&mut rand::rng()).unwrap();
        let collection = db.seed_collection(&col_name, &user.id).await?;
        collections.insert(collection.id, collection);
        pb.inc(1);
    }
    pb.finish_with_message("Created collections");
    info!(
        "Created {} collections in {:.2?}",
        args.collections,
        start.elapsed()
    );

    // 3. Create randomized Functions
    let decorators = Decorator::iter().collect::<Vec<_>>();

    let mut functions = Vec::new();
    let start = Instant::now();
    let pb = ProgressBar::new(args.functions as u64);
    pb.set_style(pb_style.clone());
    pb.set_message("Creating functions...");
    for i in 0..args.functions {
        let func_name = FunctionName::try_from(format!("load_func_{i}"))?;
        let decorator = decorators.choose(&mut rand::rng()).unwrap();

        // we will always end up with dag constructions given that we can only depend on previous functions
        // build deps
        let dependency_functions = functions
            .sample(&mut rand::rng(), rand::rng().random_range(0..=5))
            .cloned()
            .collect::<Vec<FunctionDB>>();
        let mut dependencies = Vec::new();
        for dependency_function in dependency_functions {
            let tables = db
                .fetch_all::<TableDBWithNames>(&dependency_function.id)
                .await?;
            if let Some(dependency_table) = tables.choose(&mut rand::rng()) {
                dependencies.push(TableDependencyDto::try_from(format!(
                    "{}/{}",
                    dependency_table.collection, dependency_table.name
                ))?);
            }
        }

        // build triggers
        let trigger_functions = functions
            .sample(&mut rand::rng(), rand::rng().random_range(0..=5))
            .cloned()
            .collect::<Vec<FunctionDB>>();
        let mut triggers = Vec::new();
        for trigger_function in trigger_functions {
            let tables = db
                .fetch_all::<TableDBWithNames>(&trigger_function.id)
                .await?;
            if let Some(trigger_table) = tables.choose(&mut rand::rng()) {
                triggers.push(TableTriggerDto::try_from(format!(
                    "{}/{}",
                    trigger_table.collection, trigger_table.name
                ))?);
            }
        }

        // build tables
        let mut tables = Vec::new();
        for j in 0..rand::rng().random_range(0..=5) {
            let table = TableNameDto::try_from(format!("load_table_{i}_{j}"))?;
            tables.push(table);
        }

        debug!(
            "For function {}, creating {} tables, {} dependencies and {} triggers",
            func_name,
            tables.len(),
            dependencies.len(),
            triggers.len(),
        );

        let create = FunctionRegister::builder()
            .name(func_name)
            .try_description("description")?
            .bundle_id(BundleId::default())
            .try_snippet("snippet")?
            .decorator(decorator)
            .dependencies(dependencies)
            .triggers(triggers)
            .tables(tables)
            .try_runtime_values("{}")?
            .reuse_frozen_tables(false)
            .build()?;

        let (_, coll) = collections.iter().choose(&mut rand::rng()).unwrap();
        let function = db.seed_function(coll, &create).await?;
        functions.push(function);
        pb.inc(1);
    }
    pb.finish_with_message("Created functions");
    info!(
        "Created {} functions in {:.2?}",
        functions.len(),
        start.elapsed()
    );

    // 3. Create randomized Executions, Transactions and Data Versions
    let mut executions = Vec::new();
    let mut transactions = Vec::new();
    let mut data_versions = Vec::new();
    let start = Instant::now();

    let transaction_mapper = TransactionBy::default();
    let pb = ProgressBar::new(args.executions as u64);
    pb.set_style(pb_style);
    pb.set_message("Creating executions, transactions and data versions...");
    for _ in 0..rand::rng().random_range(0..=args.executions) {
        let function = functions.choose(&mut rand::rng()).unwrap();
        let collection = collections.get(&function.collection_id).unwrap();
        let tables = db.fetch_all::<TableDB>(&function.id).await?;
        let execution = db.seed_execution(function).await?;

        for _ in 0..rand::rng().random_range(0..=args.transactions) {
            let node = FunctionNode {
                collection_id: collection.id,
                collection: collection.name.clone(),
                function_id: function.function_id,
                function_version_id: function.id.clone(),
                name: function.name.clone(),
                enabled: Default::default(),
            };

            let transaction = db
                .seed_transaction(&execution, &transaction_mapper.key(&node)?)
                .await?;

            let function_run = db
                .seed_function_run(
                    collection,
                    function,
                    &execution,
                    &transaction,
                    &FunctionRunStatus::Committed,
                )
                .await?;

            for table in &tables {
                let data_version = db
                    .seed_table_data_version(
                        collection,
                        &execution,
                        &transaction,
                        &function_run,
                        table,
                    )
                    .await;
                data_versions.push(data_version);
            }
            transactions.push(transaction);
        }
        executions.push(execution);
        pb.inc(1);
    }
    pb.finish_with_message("Created executions, transactions and data versions");
    info!(
        "Created {} executions, {} transactions and {} data_versions in {:.2?}",
        executions.len(),
        transactions.len(),
        data_versions.len(),
        start.elapsed()
    );

    info!("Total load test time: {:.2?}", start_total.elapsed());
    Ok(())
}
