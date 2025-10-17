use eventsource_stream::Event as MessageEvent;
use reqwest::Response;
use futures_core::stream::Stream;
#[cfg(feature = "amazon-eventstream")]
use tracing::debug;
use std::pin::Pin;

pub type ParsedEventStream = Pin<Box<dyn Stream<Item = Result<MessageEvent, Box<dyn std::error::Error + Send + Sync>>> + Send>>;

/// Trait for parsing different content types into events
pub trait ContentParser: Send + Sync {
    /// Check if this parser can handle the given content type
    fn can_parse(&self, content_type: &str) -> bool;
    
    /// Parse the response into a stream of events
    fn parse(&self, response: Response) -> Result<ParsedEventStream, Box<dyn std::error::Error + Send + Sync>>;
    
    /// Get the name/description of this parser
    fn name(&self) -> &'static str;
}

/// Registry for content type parsers
#[derive(Default)]
pub struct ParserRegistry {
    parsers: Vec<Box<dyn ContentParser>>,
}

impl ParserRegistry {
    pub fn new() -> Self {
        Self {
            parsers: Vec::new(),
        }
    }
    
    /// Register a new parser
    pub fn register<P: ContentParser + 'static>(&mut self, parser: P) {
        self.parsers.push(Box::new(parser));
    }
    
    /// Find a parser that can handle the given content type
    pub fn find_parser(&self, content_type: &str) -> Option<&dyn ContentParser> {
        debug!("(eventsource) ParserRegistry::find_parser called with content_type: '{}'", content_type);
        debug!("(eventsource) Available parsers: {:?}", self.parsers.iter().map(|p| p.name()).collect::<Vec<_>>());
        
        let found_parser = self.parsers.iter()
            .find(|p| {
                let can_parse = p.can_parse(content_type);
                debug!("(eventsource) Parser '{}' can_parse('{}') = {}", p.name(), content_type, can_parse);
                can_parse
            })
            .map(|p| p.as_ref());
            
        if let Some(parser) = found_parser {
            debug!("(eventsource) Selected parser: '{}'", parser.name());
        } else {
            debug!("(eventsource) No parser found for content type: '{}'", content_type);
        }
        
        found_parser
    }
    
    /// Get all supported content types
    pub fn supported_content_types(&self) -> Vec<String> {
        // This would need to be enhanced if parsers support multiple content types
        // For now, just return some common ones
        let mut types = vec!["text/event-stream".to_string()];
        
        #[cfg(feature = "amazon-eventstream")]
        types.push("application/vnd.amazon.eventstream".to_string());
        
        types
    }
}

/// Default SSE parser for text/event-stream
pub struct SseParser;

impl ContentParser for SseParser {
    fn can_parse(&self, content_type: &str) -> bool {
        content_type.starts_with("text/event-stream")
    }
    
    fn parse(&self, response: Response) -> Result<ParsedEventStream, Box<dyn std::error::Error + Send + Sync>> {
        use eventsource_stream::Eventsource;
        use futures_util::StreamExt;
        
        let stream = response.bytes_stream().eventsource().map(|result| {
            result.map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        });
        
        Ok(Box::pin(stream))
    }
    
    fn name(&self) -> &'static str {
        "Server-Sent Events (SSE) Parser"
    }
}

#[cfg(feature = "amazon-eventstream")]
/// Parser for Amazon EventStream format
pub struct AmazonEventStreamParser;

#[cfg(feature = "amazon-eventstream")]
impl ContentParser for AmazonEventStreamParser {
    fn can_parse(&self, content_type: &str) -> bool {
        content_type.contains("amazon.eventstream") || content_type.ends_with("eventstream")
    }
    
    fn parse(&self, response: Response) -> Result<ParsedEventStream, Box<dyn std::error::Error + Send + Sync>> {
        use futures_util::StreamExt;
        use crate::amazon_eventstream::parse_eventstream_messages;
        
        debug!("(eventsource) AmazonEventStreamParser::parse called");
        
        // We need to collect chunks and parse them as Amazon EventStream
        let stream = response.bytes_stream()
            .scan(Vec::new(), |buffer, chunk_result| {
                let events = match chunk_result {
                    Ok(chunk) => {
                        debug!("(eventsource) received chunk of {} bytes", chunk.len());
                        buffer.extend_from_slice(&chunk);
                        debug!("(eventsource) buffer now has {} bytes", buffer.len());
                        
                        // Try to parse accumulated data
                        let parsed_events = parse_eventstream_messages(buffer);
                        if !parsed_events.is_empty() {
                            debug!("(eventsource) parsed {} events, clearing buffer", parsed_events.len());
                            buffer.clear(); // Clear buffer after successful parsing
                            parsed_events.into_iter().map(Ok).collect::<Vec<_>>()
                        } else {
                            debug!("(eventsource) no events parsed, continuing to accumulate");
                            vec![] // Continue accumulating
                        }
                    }
                    Err(e) => {
                        debug!("(eventsource) chunk error: {:?}", e);
                        vec![Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>)]
                    }
                };
                
                async move { Some(events) }
            })
            .flat_map(futures_util::stream::iter);
            
        Ok(Box::pin(stream))
    }
    
    fn name(&self) -> &'static str {
        "Amazon EventStream Parser"
    }
}

/// Builder for creating a parser registry with default parsers
pub struct ParserRegistryBuilder {
    registry: ParserRegistry,
}

impl Default for ParserRegistryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ParserRegistryBuilder {
    pub fn new() -> Self {
        Self {
            registry: ParserRegistry::new(),
        }
    }
    
    /// Add default parsers (SSE and Amazon EventStream if enabled)
    pub fn with_default_parsers(mut self) -> Self {
        self.registry.register(SseParser);
        #[cfg(feature = "amazon-eventstream")]
        self.registry.register(AmazonEventStreamParser);
        self
    }
    
    /// Add a custom parser
    pub fn with_parser<P: ContentParser + 'static>(mut self, parser: P) -> Self {
        self.registry.register(parser);
        self
    }
    
    /// Build the registry
    pub fn build(self) -> ParserRegistry {
        self.registry
    }
}