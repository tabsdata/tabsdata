//
//  Copyright 2024 Tabs Data Inc.
//

// #[cfg(test)]
// mod tests {
//     use std::sync::Arc;
//     use td_typing::service_type;
//
//     #[service_type]
//     struct Dataset(String);
//
//     #[test]
//     fn test() {
//         let _ = Dataset("dataset".to_string());
//
//         let dataset = Dataset::new("dataset".to_string());
//         let _: &String = &dataset;
//         let _: &str = &dataset;
//         let _: String = (&dataset).into();
//         let value: String = dataset.into();
//         assert_eq!(value, "dataset");
//
//         let dataset = Arc::new(Dataset::new("dataset".to_string()));
//         let _: &String = &dataset;
//         let _: &str = &dataset;
//         assert_eq!(value, "dataset");
//     }
// }
