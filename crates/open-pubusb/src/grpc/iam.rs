//! `google.iam.v1.IAMPolicy` — out of scope, same as the official Pub/Sub
//! emulator, which likewise leaves IAM unsupported. Every method returns
//! `UNIMPLEMENTED`.

use open_pubusb_proto::iam::v1 as pb;
use tonic::{Request, Response, Status};

#[derive(Debug, Default)]
pub struct IamPolicyImpl;

fn unimplemented<T>() -> Result<Response<T>, Status> {
    Err(Status::unimplemented(
        "IAMPolicy is out of scope for this server",
    ))
}

#[tonic::async_trait]
impl pb::iam_policy_server::IamPolicy for IamPolicyImpl {
    async fn set_iam_policy(
        &self,
        _request: Request<pb::SetIamPolicyRequest>,
    ) -> Result<Response<pb::Policy>, Status> {
        unimplemented()
    }

    async fn get_iam_policy(
        &self,
        _request: Request<pb::GetIamPolicyRequest>,
    ) -> Result<Response<pb::Policy>, Status> {
        unimplemented()
    }

    async fn test_iam_permissions(
        &self,
        _request: Request<pb::TestIamPermissionsRequest>,
    ) -> Result<Response<pb::TestIamPermissionsResponse>, Status> {
        unimplemented()
    }
}
