use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use hyper::{body::to_bytes, Body, Method, Request};
use tokio::sync::{mpsc, oneshot};

use crate::{
    db::{self, SubmittedEvent},
    event::EventWrapper,
    notice::Notice,
    repo::NostrRepo,
    server::{convert_to_msg, NostrMessage},
};

fn parse_query_params(query_string: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();

    if query_string.is_empty() {
        return params;
    }

    for pair in query_string.split('&') {
        if let Some((key, value)) = pair.split_once('=') {
            params.insert(key.to_string(), value.to_string());
        }
    }

    params
}

// TODO: more granular error handling
pub(crate) async fn handle_request(
    request: Request<Body>,
    repo: Arc<dyn NostrRepo>,
    event_tx: mpsc::Sender<SubmittedEvent>,
) -> Result<String> {
    let method = request.method().clone();
    match method {
        Method::GET => {
            let query_params = parse_query_params(request.uri().query().unwrap_or_default());
            let message = query_params
                .get("filter")
                .ok_or(anyhow::anyhow!("Message not found"))?;
            let message_string = String::from_utf8(hex::decode(message)?)?;
            let nostr_message = convert_to_msg(&message_string, None)?;
            match nostr_message {
                NostrMessage::SubMsg(sub) => {
                    println!("======= Subscription received via OHTTP: {:?}", sub.id);
                    // Create a channel for query results
                    let (query_tx, mut query_rx) = mpsc::channel::<db::QueryResult>(1000);
                    // Create a channel to abandon the query if needed
                    let (_abandon_query_tx, abandon_query_rx) = oneshot::channel::<()>();

                    let repo_clone = repo.clone();
                    let sub_clone = sub.clone();
                    tokio::spawn(async move {
                        if let Err(e) = repo_clone
                            .query_subscription(
                                sub_clone,
                                "ohttp_client".to_string(),
                                query_tx,
                                abandon_query_rx,
                            )
                            .await
                        {
                            eprintln!("OHTTP subscription query error: {:?}", e);
                        }
                    });
                    let mut events = Vec::new();
                    let timeout = tokio::time::Duration::from_secs(10);

                    loop {
                        tokio::select! {
                            // Receive query results
                            result = query_rx.recv() => {
                                match result {
                                    Some(query_result) => {
                                        // Add event to our response
                                        events.push(query_result.event);
                                    }
                                    None => {
                                        // Channel closed, query finished
                                        break;
                                    }
                                }
                            }
                            // Timeout reached
                            _ = tokio::time::sleep(timeout) => {
                                println!("OHTTP subscription timeout reached");
                                break;
                            }
                        }
                    }
                    println!("======= Events: {:?}", events);

                    // Join all events with newlines for the response
                    let response_data = events.join("\n");
                    return Ok(response_data.to_string());
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Expected a subscription message for GET request"
                    ));
                }
            }
        }
        Method::POST => {
            let body = to_bytes(request.into_body()).await?;
            let body = String::from_utf8(body.to_vec())?;
            let nostr_message = convert_to_msg(&body, None)?;

            // posting an event, expecting an event event
            match nostr_message {
                NostrMessage::EventMsg(event_command) => {
                    println!("======= Event received via OHTTP: {:?}", event_command);
                    let parsed: crate::error::Result<EventWrapper> = event_command.into();
                    match parsed {
                        Ok(EventWrapper::WrappedEvent(e)) => {
                            // Create a notice channel for OHTTP responses
                            let (notice_tx, mut notice_rx) =
                                tokio::sync::mpsc::channel::<Notice>(1);

                            let submit_event = SubmittedEvent {
                                event: e.clone(),
                                notice_tx,
                                source_ip: "foo bar".to_string(),
                                origin: None,
                                user_agent: None,
                                auth_pubkey: None,
                            };

                            // Send to database writer
                            if let Err(e) = event_tx.send(submit_event).await {
                                println!("======= Failed to send event to database: {:?}", e);
                            }

                            // Wait for processing result and log any notices
                            if let Some(notice) = notice_rx.recv().await {
                                match notice {
                                    Notice::Message(msg) => {
                                        println!("======= Event processing message: {}", msg)
                                    }
                                    Notice::EventResult(result) => println!(
                                        "======= Event processing result: {} - {}",
                                        result.status.prefix(),
                                        result.msg
                                    ),
                                    Notice::AuthChallenge(challenge) => println!(
                                        "======= Event processing auth challenge: {}",
                                        challenge
                                    ),
                                }
                                return Ok(String::new());
                            } else {
                                return Err(anyhow::anyhow!("Failed to send event to database"));
                            }
                        }
                        Ok(EventWrapper::WrappedAuth(_)) => {
                            return Err(anyhow::anyhow!("AUTH events are not supported"));
                        }
                        Err(e) => {
                            return Err(anyhow::anyhow!("Invalid event: {:?}", e));
                        }
                    }
                }

                _ => {
                    return Err(anyhow::anyhow!(
                        "Expected an event message for POST request"
                    ));
                }
            }
        }
        _ => {
            return Err(anyhow::anyhow!("Unsupported HTTP method"));
        }
    }
}
