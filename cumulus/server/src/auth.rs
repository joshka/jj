//! Bearer-token authentication (`cumulus/docs/SPEC.md` §6.3).

use std::collections::HashMap;
use std::sync::Arc;

use tonic::Request;
use tonic::Status;
use tonic::service::Interceptor;

/// The authenticated user, attached to request extensions. Only logged in
/// v1 (per-repo ACLs are out of scope, §1 non-goals).
#[derive(Clone, Debug)]
pub struct AuthedUser(pub String);

/// Checks `authorization: Bearer <token>` against the configured token
/// map. With no tokens configured, all requests pass unauthenticated
/// (development mode).
#[derive(Clone, Debug)]
pub struct AuthInterceptor {
    tokens: Arc<HashMap<String, String>>,
}

impl AuthInterceptor {
    /// Creates an interceptor for the given token-to-user map.
    pub fn new(tokens: HashMap<String, String>) -> Self {
        Self {
            tokens: Arc::new(tokens),
        }
    }
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if self.tokens.is_empty() {
            return Ok(request);
        }
        let token = request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or_else(|| Status::unauthenticated("missing bearer token"))?;
        let Some(user) = self.tokens.get(token) else {
            return Err(Status::unauthenticated("invalid bearer token"));
        };
        tracing::debug!(user, "authenticated request");
        request.extensions_mut().insert(AuthedUser(user.clone()));
        Ok(request)
    }
}
