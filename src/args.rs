use std::collections::HashMap;

#[derive(Debug)]
pub struct ParsedArgs {
    pub named: HashMap<String, String>,
    pub positional: Vec<String>,
}

pub fn parse_args(args: Vec<String>) -> ParsedArgs {
    let mut named = HashMap::new();
    let mut positional = Vec::new();

    for arg in args {
        if let Some(eq_pos) = arg.find('=') {
            let key = arg[..eq_pos].trim().to_string().to_uppercase();
            let value = arg[eq_pos + 1..]
                .trim_matches(|c| c == '\'' || c == '"')
                .to_string();
            named.insert(key, value);
        } else {
            positional.push(arg.trim_matches(|c| c == '\'' || c == '"').to_string());
        }
    }

    ParsedArgs { named, positional }
}

#[test]
fn test_named_arguments() {
    let input = vec![
        "name=John".to_string(),
        "age='30'".to_string(),
        "city=\"New York\"".to_string(),
    ];
    let result = parse_args(input);
    let mut expected_named = HashMap::new();
    expected_named.insert("NAME".to_string(), "John".to_string());
    expected_named.insert("AGE".to_string(), "30".to_string());
    expected_named.insert("CITY".to_string(), "New York".to_string());

    assert_eq!(result.named, expected_named);
    assert_eq!(result.positional, Vec::<String>::new());
}

#[test]
fn test_positional_arguments() {
    let input = vec![
        "foo".to_string(),
        "'bar'".to_string(),
        "\"baz\"".to_string(),
    ];
    let result = parse_args(input);
    let expected_positional = vec!["foo".to_string(), "bar".to_string(), "baz".to_string()];

    assert!(result.named.is_empty());
    assert_eq!(result.positional, expected_positional);
}

#[test]
fn test_mixed_arguments() {
    let input = vec![
        "user=admin".to_string(),
        "password='secret'".to_string(),
        "run".to_string(),
        "'--force'".to_string(),
    ];
    let result = parse_args(input);
    let mut expected_named = HashMap::new();
    expected_named.insert("USER".to_string(), "admin".to_string());
    expected_named.insert("PASSWORD".to_string(), "secret".to_string());
    let expected_positional = vec!["run".to_string(), "--force".to_string()];

    assert_eq!(result.named, expected_named);
    assert_eq!(result.positional, expected_positional);
}

#[test]
fn test_empty_input() {
    let input: Vec<String> = Vec::new();
    let result = parse_args(input);
    assert!(result.named.is_empty());
    assert!(result.positional.is_empty());
}
