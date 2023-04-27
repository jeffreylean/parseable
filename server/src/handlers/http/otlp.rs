use std::ops::Deref;

use crate::metadata::STREAM_INFO;
use crate::utils::header_parsing::{collect_labelled_headers, ParseHeaderError};
use crate::{
    event::{
        self,
        format::{self, EventFormat},
        Event,
    },
    handlers::{PREFIX_META, PREFIX_TAGS, SEPARATOR, STREAM_NAME_HEADER_KEY},
    utils::header_parsing::{collect_labelled_headers, ParseHeaderError},
};
use actix_protobuf::{ProtoBuf, ProtoBufResponseBuilder};
use actix_web::{Error, HttpRequest, HttpResponse, Responder};
use bytes::BytesMut;
use prost::Message;
use serde_json::Value;

use super::ingest::PostError;

mod trace {
    include!(concat!(env!("OUT_DIR"), "/trace.rs"));
}

mod common {
    include!(concat!(env!("OUT_DIR"), "/common.rs"));
}

mod resource {
    include!(concat!(env!("OUT_DIR"), "/resource.rs"));
}

pub async fn traces(
    req: HttpRequest,
    body: ProtoBuf<trace::ExportTraceServiceRequest>,
) -> Result<HttpResponse, PostError> {
    // Get stream name
    if req.headers().contains_key(STREAM_NAME_HEADER_KEY) {
        let stream_name = req
            .headers()
            .get(STREAM_NAME_HEADER_KEY)
            .unwrap()
            .to_str()
            .unwrap()
            .to_owned();

        if let Err(e) = super::logstream::create_stream_if_not_exists(&stream_name).await {
            return Err(PostError::CreateStream(e.into()));
        }
        let size = body.encoded_len();
        let trace_service_req = body.deref().to_owned();
    }
    let resp = trace::ExportTraceServiceResponse {
        partial_success: None,
    };
    Ok(HttpResponse::Ok().finish())
}

async fn push_logs(stream_name: String, req: HttpRequest, body: Value, size: usize) {
    // Into event batch
    let tags = collect_labelled_headers(&req, PREFIX_TAGS, SEPARATOR);
    let metadata = collect_labelled_headers(&req, PREFIX_META, SEPERATOR);
    // Create an event
    let json_event = format::json::Event {
        data: body,
        tags,
        metadata,
    };

    let rb = json_event.into_recordbatch(&STREAM_INFO.schema(&stream_name).unwrap());
    event::Event {
        rb,
        stream_name,
        origin_format: "json",
        origin_size: size as u64,
    }
    .process()
    .await?;

    Ok(())
}
