//
// Copyright 2026 Tabsdata Inc.
//

use ta_services::factory::service_factory;
use ta_services::service::Ctx;
use td_error::TdError;
use td_objects::dxo::system::Instance;
use td_objects::types::basic::InstanceId;

#[service_factory]
async fn instance(instance_id: Ctx<InstanceId>, _request: ()) -> Result<Instance, TdError> {
    Ok(Instance {
        instance_id: (*instance_id).clone(),
    })
}

#[cfg(test)]
mod tests {
    use crate::Services;
    use ta_services::service::Ctx;
    use td_objects::types::basic::InstanceId;

    #[tokio::test]
    async fn test_instance_service() {
        let id = InstanceId::try_from("0b5c3f0e-8a52-4d2b-9c41-6f1e2d3c4b5a").unwrap();
        let services = Services {
            instance_id: Ctx::new(id.clone()),
            ..Services::default()
        };
        assert_eq!(services.instance(()).await.unwrap().instance_id, id);
    }
}
