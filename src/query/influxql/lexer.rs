//! InfluxQL Lexer (Tokenizer)
//!
//! Converts InfluxQL query string into a stream of tokens.

use std::iter::Peekable;
use std::str::Chars;

/// Token types
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // Keywords
    Select,
    From,
    Where,
    And,
    Or,
    Not,
    As,
    GroupBy,
    OrderBy,
    Asc,
    Desc,
    Limit,
    Offset,
    SLimit,
    SOffset,
    Fill,
    Show,
    Measurements,
    Databases,
    Tag,
    Field,
    Keys,
    Values,
    RetentionPolicies,
    On,
    With,
    Key,
    In,
    Time,
    Tz,
    True,
    False,
    Null,
    None,
    Previous,
    Linear,
    Now,

    // Literals
    Ident(String),
    Integer(i64),
    Float(f64),
    String(String),
    Regex(String),
    Duration(i64, String), // value and unit

    // Operators
    Plus,
    Minus,
    Star,
    Slash,
    Percent,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    RegexMatch,    // =~
    RegexNotMatch, // !~

    // Punctuation
    LParen,
    RParen,
    Comma,
    Dot,
    DoubleColon,
    Semicolon,

    // Special
    Eof,
}

/// Lexer error
#[derive(Debug, Clone, PartialEq)]
pub struct LexerError {
    pub message: String,
    pub position: usize,
}

impl std::fmt::Display for LexerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Lexer error at position {}: {}", self.position, self.message)
    }
}

impl std::error::Error for LexerError {}

/// InfluxQL Lexer
pub struct Lexer<'a> {
    input: &'a str,
    chars: Peekable<Chars<'a>>,
    position: usize,
    token_positions: Vec<usize>,
}

impl<'a> Lexer<'a> {
    pub fn new(input: &'a str) -> Self {
        Self {
            input,
            chars: input.chars().peekable(),
            position: 0,
            token_positions: Vec::new(),
        }
    }

    /// Tokenize the entire input
    pub fn tokenize(&mut self) -> Result<Vec<Token>, LexerError> {
        let mut tokens = Vec::new();
        self.token_positions.clear();
        loop {
            self.skip_whitespace();
            self.token_positions.push(self.position);
            let token = self.next_token()?;
            if token == Token::Eof {
                tokens.push(token);
                break;
            }

            // After =~ or !~, scan a regex literal if the next char is /
            if (token == Token::RegexMatch || token == Token::RegexNotMatch)
                && self.peek_after_whitespace() == Some('/')
            {
                tokens.push(token);
                self.skip_whitespace();
                self.token_positions.push(self.position);
                let (pattern, end_pos) = scan_regex(self.input, self.position)?;
                // Advance the lexer past the regex
                while self.position < end_pos {
                    self.advance();
                }
                tokens.push(Token::Regex(pattern));
                continue;
            }

            tokens.push(token);
        }
        Ok(tokens)
    }

    /// Get the byte positions of each token in the original input
    pub fn token_positions(&self) -> &[usize] {
        &self.token_positions
    }

    fn next_token(&mut self) -> Result<Token, LexerError> {
        self.skip_whitespace();

        match self.peek() {
            None => Ok(Token::Eof),
            Some(c) => match c {
                // Single character tokens
                '(' => self.single_char(Token::LParen),
                ')' => self.single_char(Token::RParen),
                ',' => self.single_char(Token::Comma),
                ';' => self.single_char(Token::Semicolon),
                '+' => self.single_char(Token::Plus),
                '-' => self.single_char(Token::Minus),
                '*' => self.single_char(Token::Star),
                '%' => self.single_char(Token::Percent),
                '.' => self.single_char(Token::Dot),

                // Multi-character operators
                '/' => {
                    self.advance();
                    if self.peek() == Some('/') {
                        // Line comment
                        self.skip_line();
                        self.next_token()
                    } else {
                        Ok(Token::Slash)
                    }
                }
                '=' => {
                    self.advance();
                    if self.peek() == Some('~') {
                        self.advance();
                        Ok(Token::RegexMatch)
                    } else {
                        Ok(Token::Eq)
                    }
                }
                '!' => {
                    self.advance();
                    match self.peek() {
                        Some('=') => {
                            self.advance();
                            Ok(Token::Ne)
                        }
                        Some('~') => {
                            self.advance();
                            Ok(Token::RegexNotMatch)
                        }
                        _ => Err(self.error("Expected '=' or '~' after '!'")),
                    }
                }
                '<' => {
                    self.advance();
                    match self.peek() {
                        Some('=') => {
                            self.advance();
                            Ok(Token::Le)
                        }
                        Some('>') => {
                            self.advance();
                            Ok(Token::Ne)
                        }
                        _ => Ok(Token::Lt),
                    }
                }
                '>' => {
                    self.advance();
                    if self.peek() == Some('=') {
                        self.advance();
                        Ok(Token::Ge)
                    } else {
                        Ok(Token::Gt)
                    }
                }
                ':' => {
                    self.advance();
                    if self.peek() == Some(':') {
                        self.advance();
                        Ok(Token::DoubleColon)
                    } else {
                        Err(self.error("Expected ':' after ':'"))
                    }
                }

                // String literals
                '\'' | '"' => self.scan_string(),

                // Regex literals
                '~' => {
                    self.advance();
                    Err(self.error("Unexpected '~'"))
                }

                // Numbers
                '0'..='9' => self.scan_number(),

                // Identifiers and keywords
                'a'..='z' | 'A'..='Z' | '_' => self.scan_identifier(),

                _ => {
                    self.advance();
                    Err(self.error(&format!("Unexpected character: '{}'", c)))
                }
            },
        }
    }

    fn peek(&mut self) -> Option<char> {
        self.chars.peek().copied()
    }

    fn advance(&mut self) -> Option<char> {
        let c = self.chars.next();
        if c.is_some() {
            self.position += 1;
        }
        c
    }

    fn single_char(&mut self, token: Token) -> Result<Token, LexerError> {
        self.advance();
        Ok(token)
    }

    fn skip_whitespace(&mut self) {
        while let Some(c) = self.peek() {
            if c.is_whitespace() {
                self.advance();
            } else {
                break;
            }
        }
    }

    /// Peek at the next non-whitespace character without consuming
    fn peek_after_whitespace(&self) -> Option<char> {
        let remaining = &self.input[self.position..];
        remaining.chars().find(|c| !c.is_whitespace())
    }

    fn skip_line(&mut self) {
        while let Some(c) = self.peek() {
            if c == '\n' {
                break;
            }
            self.advance();
        }
    }

    fn scan_string(&mut self) -> Result<Token, LexerError> {
        let quote = self.advance().unwrap();
        let mut value = String::new();

        loop {
            match self.advance() {
                None => return Err(self.error("Unterminated string")),
                Some(c) if c == quote => break,
                Some('\\') => {
                    match self.advance() {
                        Some('n') => value.push('\n'),
                        Some('t') => value.push('\t'),
                        Some('r') => value.push('\r'),
                        Some('\\') => value.push('\\'),
                        Some(c) if c == quote => value.push(c),
                        Some(c) => {
                            value.push('\\');
                            value.push(c);
                        }
                        None => return Err(self.error("Unterminated string escape")),
                    }
                }
                Some(c) => value.push(c),
            }
        }

        // In InfluxQL, double-quoted strings are identifiers, single-quoted are string literals
        if quote == '"' {
            Ok(Token::Ident(value))
        } else {
            Ok(Token::String(value))
        }
    }

    fn scan_number(&mut self) -> Result<Token, LexerError> {
        let start = self.position;
        let mut has_dot = false;
        let mut has_exp = false;

        while let Some(c) = self.peek() {
            match c {
                '0'..='9' => {
                    self.advance();
                }
                '.' if !has_dot && !has_exp => {
                    has_dot = true;
                    self.advance();
                }
                'e' | 'E' if !has_exp => {
                    has_exp = true;
                    self.advance();
                    if let Some(c) = self.peek() {
                        if c == '+' || c == '-' {
                            self.advance();
                        }
                    }
                }
                // Duration suffix
                'n' | 'u' | 'µ' | 'm' | 's' | 'h' | 'd' | 'w' if !has_dot && !has_exp => {
                    return self.scan_duration(start);
                }
                _ => break,
            }
        }

        let text = &self.input[start..self.position];
        if has_dot || has_exp {
            text.parse::<f64>()
                .map(Token::Float)
                .map_err(|_| self.error(&format!("Invalid float: {}", text)))
        } else {
            text.parse::<i64>()
                .map(Token::Integer)
                .map_err(|_| self.error(&format!("Invalid integer: {}", text)))
        }
    }

    fn scan_duration(&mut self, start: usize) -> Result<Token, LexerError> {
        let num_str = &self.input[start..self.position];
        let value: i64 = num_str
            .parse()
            .map_err(|_| self.error(&format!("Invalid duration value: {}", num_str)))?;

        let mut unit = String::new();
        while let Some(c) = self.peek() {
            if c.is_alphabetic() || c == 'µ' {
                unit.push(c);
                self.advance();
            } else {
                break;
            }
        }

        // Validate unit
        match unit.as_str() {
            "ns" | "u" | "µ" | "us" | "ms" | "s" | "m" | "h" | "d" | "w" => {
                Ok(Token::Duration(value, unit))
            }
            _ => Err(self.error(&format!("Invalid duration unit: {}", unit))),
        }
    }

    fn scan_identifier(&mut self) -> Result<Token, LexerError> {
        let start = self.position;

        while let Some(c) = self.peek() {
            if c.is_alphanumeric() || c == '_' {
                self.advance();
            } else {
                break;
            }
        }

        let text = &self.input[start..self.position];
        Ok(self.keyword_or_ident(text))
    }

    fn keyword_or_ident(&self, text: &str) -> Token {
        match text.to_uppercase().as_str() {
            "SELECT" => Token::Select,
            "FROM" => Token::From,
            "WHERE" => Token::Where,
            "AND" => Token::And,
            "OR" => Token::Or,
            "NOT" => Token::Not,
            "AS" => Token::As,
            "GROUP" => Token::GroupBy, // Will be combined with BY in parser
            "ORDER" => Token::OrderBy, // Will be combined with BY in parser
            "BY" => Token::Ident("BY".to_string()), // Handled specially
            "ASC" => Token::Asc,
            "DESC" => Token::Desc,
            "LIMIT" => Token::Limit,
            "OFFSET" => Token::Offset,
            "SLIMIT" => Token::SLimit,
            "SOFFSET" => Token::SOffset,
            "FILL" => Token::Fill,
            "SHOW" => Token::Show,
            "MEASUREMENTS" => Token::Measurements,
            "DATABASES" => Token::Databases,
            "TAG" => Token::Tag,
            "FIELD" => Token::Field,
            "KEYS" => Token::Keys,
            "VALUES" => Token::Values,
            "RETENTION" => Token::RetentionPolicies, // Combined with POLICIES
            "POLICIES" => Token::Ident("POLICIES".to_string()),
            "ON" => Token::On,
            "WITH" => Token::With,
            "KEY" => Token::Key,
            "IN" => Token::In,
            "TIME" => Token::Time,
            "TZ" => Token::Tz,
            "TRUE" => Token::True,
            "FALSE" => Token::False,
            "NULL" => Token::Null,
            "NONE" => Token::None,
            "PREVIOUS" => Token::Previous,
            "LINEAR" => Token::Linear,
            "NOW" => Token::Now,
            _ => Token::Ident(text.to_string()),
        }
    }

    fn error(&self, message: &str) -> LexerError {
        LexerError {
            message: message.to_string(),
            position: self.position,
        }
    }
}

/// Scan a regex pattern starting with /
pub fn scan_regex(input: &str, start: usize) -> Result<(String, usize), LexerError> {
    let bytes = input.as_bytes();
    let mut i = start;

    if i >= bytes.len() || bytes[i] != b'/' {
        return Err(LexerError {
            message: "Expected '/' to start regex".to_string(),
            position: start,
        });
    }
    i += 1;

    let mut pattern = String::new();
    let mut escaped = false;

    while i < bytes.len() {
        let c = bytes[i] as char;
        if escaped {
            pattern.push(c);
            escaped = false;
        } else if c == '\\' {
            pattern.push(c);
            escaped = true;
        } else if c == '/' {
            return Ok((pattern, i + 1));
        } else {
            pattern.push(c);
        }
        i += 1;
    }

    Err(LexerError {
        message: "Unterminated regex".to_string(),
        position: start,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let mut lexer = Lexer::new("SELECT * FROM cpu");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(
            tokens,
            vec![
                Token::Select,
                Token::Star,
                Token::From,
                Token::Ident("cpu".to_string()),
                Token::Eof,
            ]
        );
    }

    #[test]
    fn test_select_with_where() {
        let mut lexer = Lexer::new("SELECT value FROM cpu WHERE host = 'server01'");
        let tokens = lexer.tokenize().unwrap();
        assert!(tokens.contains(&Token::Where));
        assert!(tokens.contains(&Token::Eq));
        assert!(tokens.contains(&Token::String("server01".to_string())));
    }

    #[test]
    fn test_duration() {
        let mut lexer = Lexer::new("5m 1h 30s 100ms");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Duration(5, "m".to_string()));
        assert_eq!(tokens[1], Token::Duration(1, "h".to_string()));
        assert_eq!(tokens[2], Token::Duration(30, "s".to_string()));
        assert_eq!(tokens[3], Token::Duration(100, "ms".to_string()));
    }

    #[test]
    fn test_numbers() {
        let mut lexer = Lexer::new("42 3.14 -100 1.5e10");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Integer(42));
        assert_eq!(tokens[1], Token::Float(3.14));
        assert_eq!(tokens[2], Token::Minus);
        assert_eq!(tokens[3], Token::Integer(100));
    }

    #[test]
    fn test_operators() {
        let mut lexer = Lexer::new("=~ !~ <> <= >= + - * /");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::RegexMatch);
        assert_eq!(tokens[1], Token::RegexNotMatch);
        assert_eq!(tokens[2], Token::Ne);
        assert_eq!(tokens[3], Token::Le);
        assert_eq!(tokens[4], Token::Ge);
    }

    #[test]
    fn test_double_quoted_identifiers() {
        // Double-quoted strings should produce Token::Ident, not Token::String
        let mut lexer = Lexer::new(r#""host" = 'server01'"#);
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Ident("host".to_string()));
        assert_eq!(tokens[1], Token::Eq);
        assert_eq!(tokens[2], Token::String("server01".to_string()));
    }

    #[test]
    fn test_double_quoted_measurement() {
        let mut lexer = Lexer::new(r#"SELECT * FROM "cpu""#);
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens[0], Token::Select);
        assert_eq!(tokens[1], Token::Star);
        assert_eq!(tokens[2], Token::From);
        assert_eq!(tokens[3], Token::Ident("cpu".to_string()));
    }

    #[test]
    fn test_token_positions() {
        let mut lexer = Lexer::new("SELECT * FROM cpu");
        let _tokens = lexer.tokenize().unwrap();
        let positions = lexer.token_positions();
        assert_eq!(positions[0], 0);  // SELECT
        assert_eq!(positions[1], 7);  // *
        assert_eq!(positions[2], 9);  // FROM
        assert_eq!(positions[3], 14); // cpu
    }
}
