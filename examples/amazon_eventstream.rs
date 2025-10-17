use reqwest_eventsource::EventSource;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Example of how to use EventSource with Amazon EventStream
    
    // Create a request that accepts both SSE and Amazon EventStream
    let client = reqwest::Client::new();
    let request = client
        .post("https://bedrock-runtime.us-east-1.amazonaws.com/model/anthropic.claude-sonnet-4-5-v2/invoke-with-response-stream")
        .header("Content-Type", "application/json")
        .header("Authorization", "AWS4-HMAC-SHA256 ...") // Your AWS auth header
        .body(r#"{
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1000,
            "messages": [{
                "role": "user",
                "content": "Hello!"
            }]
        }"#);

    // Create EventSource - it will automatically detect and handle Amazon EventStream format
    let mut es = EventSource::new(request)?;

    println!("Listening for events...");

    while let Some(event) = es.next().await {
        match event {
            Ok(reqwest_eventsource::Event::Open) => {
                println!("Connection established!");
            }
            Ok(reqwest_eventsource::Event::Message(message)) => {
                println!("Received: {:?}", message);
                
                // For Amazon EventStream, the event type tells us what kind of message this is
                match message.event.as_str() {
                    "message_start" => println!("Stream started"),
                    "content_block_start" => println!("Content block started"),
                    "content_block_delta" => {
                        // This contains the actual text content
                        println!("Content: {}", message.data);
                    }
                    "content_block_stop" => println!("Content block ended"),
                    "message_stop" => {
                        println!("Stream ended");
                        break;
                    }
                    _ => println!("Other event: {} - {}", message.event, message.data),
                }
            }
            Err(err) => {
                println!("Error: {}", err);
                break;
            }
        }
    }

    Ok(())
}