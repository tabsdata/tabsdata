//
// Copyright 2026 Tabsdata Inc.
//

use std::path::PathBuf;
use td_common::crypto::{SSL_CERT_PEM_FILE, SSL_KEY_PEM_FILE};
use tokio::fs;

/// A folder holding a self-signed `localhost` key and certificate.
pub(super) async fn tls_folder() -> PathBuf {
    let tls_path = td_common::tdtestdir!();

    // Write dummy PEM files (obviously, completely invalid for real use)
    let key_path = tls_path.join(SSL_KEY_PEM_FILE);
    let test_key = r#"-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC8x26tzrTioLO8
PqlcnhaKFkk+jAXc2gMzZk8/0PfIrGgtcCbf82r4OzWWUuMkJYBAHP/sljmDC1Bd
HqNj0vmdBzOGmxoPZ0BaKmwRJ1sXskD7CuG/GM5oUzV/pe45aY+IaparAhNOkK5Y
L0prV3P6FP484UqxSdZY4cw9ujhoE6lM+ScRsJxjqGBrhKYDEELXMGJPOOVqOyl1
1sWsMhjTJ8X8o3A1LMPPzbF9rTICcPryUZxnuav4ND9+dX8DByv2/ddleQdX10OW
g3Q9fi/oObBIDZF9NO92dyu4SvMBR2dBQPUXQV2GGXq8hLHfuucYacdAY4I18NyM
0T2g46K9AgMBAAECggEABejaAeENQA/xox8KDGRwMQaaveAXxI64QsO9OjKc4yBk
itGZN0cGjk/vhkMWKbjueh3+SVDNlZjafUPFhSKNs65N8l+Ix2Upz6Nxf8WjNXYc
Jc+0z+zjSe2a6IylomX30I3XpXGlRZRuuhjRjqktmq1NZSip/tagehFSzww1PQAc
yg5Q4Gd5Nq3ngHcMc+tTnYtAgK+r1c5nlNf3F6xdaO5olZAcH5kguDwz0KH9mUN6
tmnQUjExojksMyyn6PTwph8wNZhPclFfMNgie0QMJyHEp69yKtWvfvmQjrUygTOz
JYgOjUwV6i37RpEFhIDNUPdOxdJwDSY9qvMzsFejOQKBgQDlXmZ4R0Wl14BE6e1t
fYbcq6heUbEo94bJtZvr1sV+8VnP2eMsG7fLxFih9yC2F4ojPS3s/ug0mkRWlDR/
6vj12XiylQiJEwp98LWFn2x8JdSvFuSY78THwl7oHNFVLusQ5oN/gO8cN4aKOvlJ
JUWNld3JfAuuYWCxIWJaHMP8OQKBgQDSspIxK6Q2d/gLPQ74Lgz6IBFk9r02ehlj
bqM9Hy+X4TldiASyJtbvU78suWlBn4tuv5Yp9xFFsYCtMeWFXaPXtdN3IoBwduF8
F9XGA3Czz0xrBekciaviXgTglxtr4UnGLpVk3SYg9JPm1wD0De28CIFa49BUksbz
ZmqjpKaipQKBgQCutG6cYr0cCr76vqtH4HrejilaXiLwr0kNTrUKt7YKcM8V0EKG
kD44iL9x7ogN6nQfTzQx9h7sIiy3PX+Xh2RF7nVOoNG1hrlRIA1DUCETlsUe7/MC
wm5CMTyU045msav+XXX/ojd+aJSjqTPDkQ4fP+2E0GUdV7KMeH8vYAWvkQKBgEwY
/sAPmRGrJsU4Wk28pCc6qc2jaBdi5gSJkx+iQdhDGirz025cpMhvoN6QYLm42+01
+RBTEcPwJh9npBQVC/X+z895gJYd+baODUGlQHFQ77K/wb/y4Uey2WQcb2T5S2Hu
tTpmvTyt2TVIdimvVivRjpa7LSU+leiXFvDfqOeZAoGAHqLBIXL02PVPBlitDRZa
O09LIoG1amWrXq+Cb/zjCFTBIjlY1l4FCM61zbxjHNc8AxNprls1PEDj460+H1Wo
336M0v1+uiHDK1XEB3ULHtxtYImBgfbv/vXu5V0wxfeKQfMuEhlgJ1k+9lm7/vjJ
j5JcrAQs7+AoPMM8ql2UHbA=
-----END PRIVATE KEY-----"#;
    fs::write(&key_path, test_key).await.unwrap();
    eprintln!("Key pem created in {key_path:?}");

    let cert_path = tls_path.join(SSL_CERT_PEM_FILE);
    let test_crt = r#"-----BEGIN CERTIFICATE-----
MIIDDzCCAfegAwIBAgIURKOitwpZ+hf2kjUfGbCCI1uGRYEwDQYJKoZIhvcNAQEL
BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI1MDYyMDEwNDExOFoXDTI1MDcy
MDEwNDExOFowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAvMdurc604qCzvD6pXJ4WihZJPowF3NoDM2ZPP9D3yKxo
LXAm3/Nq+Ds1llLjJCWAQBz/7JY5gwtQXR6jY9L5nQczhpsaD2dAWipsESdbF7JA
+wrhvxjOaFM1f6XuOWmPiGqWqwITTpCuWC9Ka1dz+hT+POFKsUnWWOHMPbo4aBOp
TPknEbCcY6hga4SmAxBC1zBiTzjlajspddbFrDIY0yfF/KNwNSzDz82xfa0yAnD6
8lGcZ7mr+DQ/fnV/Awcr9v3XZXkHV9dDloN0PX4v6DmwSA2RfTTvdncruErzAUdn
QUD1F0Fdhhl6vISx37rnGGnHQGOCNfDcjNE9oOOivQIDAQABo1kwVzAUBgNVHREE
DTALgglsb2NhbGhvc3QwCwYDVR0PBAQDAgeAMBMGA1UdJQQMMAoGCCsGAQUFBwMB
MB0GA1UdDgQWBBT8FtjAYGqWj7t3ItutFIcvP9k32zANBgkqhkiG9w0BAQsFAAOC
AQEATZ3gLtqxabzUfXQePhS7v7gZ4gSZpU0F4h+kkiTcRYa59ean5SU8IIii9DqK
6D5IE4Uc8312hgrZAg+/Y0iOSj89bROYcTRkHbC+yPRBBVuy0kZr3gmeKkFgMfyY
KqL3o6nqLnb8FIQEFYRquuZWZIHy2BKwNPQMQo0njQjZKjbMdUcYD1WKpMIAiKOR
rqJRg/HEdiANfvvg1Qz7XdUP/HLiCpy4nzv2/kYkH/6u3A/KQBWDfAL12iF78RY2
jEHcboQd57qqDZUulA2XGxSoFBUjG7zXgx3u1757Z3HMcR1v+P/meXKIJ33ySe8U
vRcg70teydPmY1fiAURFk3gl/g==
-----END CERTIFICATE-----"#;
    fs::write(&cert_path, test_crt).await.unwrap();
    eprintln!("Certificate pem created in {cert_path:?}");
    tls_path
}
