//! `google.pubsub.v1.SchemaService` — entirely out of scope (schema registry
//! support is not provided). Every method returns `UNIMPLEMENTED` so clients
//! get a clear, correct status rather than a connection error or silent
//! no-op.

use open_pubusb_proto::pubsub::v1 as pb;
use tonic::{Request, Response, Status};

#[derive(Debug, Default)]
pub struct SchemaServiceImpl;

fn unimplemented<T>() -> Result<Response<T>, Status> {
    Err(Status::unimplemented(
        "SchemaService is out of scope for this server",
    ))
}

#[tonic::async_trait]
impl pb::schema_service_server::SchemaService for SchemaServiceImpl {
    async fn create_schema(
        &self,
        _request: Request<pb::CreateSchemaRequest>,
    ) -> Result<Response<pb::Schema>, Status> {
        unimplemented()
    }

    async fn get_schema(
        &self,
        _request: Request<pb::GetSchemaRequest>,
    ) -> Result<Response<pb::Schema>, Status> {
        unimplemented()
    }

    async fn list_schemas(
        &self,
        _request: Request<pb::ListSchemasRequest>,
    ) -> Result<Response<pb::ListSchemasResponse>, Status> {
        unimplemented()
    }

    async fn list_schema_revisions(
        &self,
        _request: Request<pb::ListSchemaRevisionsRequest>,
    ) -> Result<Response<pb::ListSchemaRevisionsResponse>, Status> {
        unimplemented()
    }

    async fn commit_schema(
        &self,
        _request: Request<pb::CommitSchemaRequest>,
    ) -> Result<Response<pb::Schema>, Status> {
        unimplemented()
    }

    async fn rollback_schema(
        &self,
        _request: Request<pb::RollbackSchemaRequest>,
    ) -> Result<Response<pb::Schema>, Status> {
        unimplemented()
    }

    async fn delete_schema_revision(
        &self,
        _request: Request<pb::DeleteSchemaRevisionRequest>,
    ) -> Result<Response<pb::Schema>, Status> {
        unimplemented()
    }

    async fn delete_schema(
        &self,
        _request: Request<pb::DeleteSchemaRequest>,
    ) -> Result<Response<pbjson_types::Empty>, Status> {
        unimplemented()
    }

    async fn validate_schema(
        &self,
        _request: Request<pb::ValidateSchemaRequest>,
    ) -> Result<Response<pb::ValidateSchemaResponse>, Status> {
        unimplemented()
    }

    async fn validate_message(
        &self,
        _request: Request<pb::ValidateMessageRequest>,
    ) -> Result<Response<pb::ValidateMessageResponse>, Status> {
        unimplemented()
    }
}
