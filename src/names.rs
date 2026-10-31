//! ファイル名の検証と、一覧表示用の整形。

use mtp_rs::mtp::DateTime;

/// デバイス上に作る名前（リネーム・新規フォルダ）を検証する。
pub fn validate_new_name(name: &str) -> Result<&str, String> {
    let name = name.trim();
    if name.is_empty() {
        return Err("名前を入力してください。".into());
    }
    if name == "." || name == ".." {
        return Err("「.」や「..」は名前に使えません。".into());
    }
    if name.contains(['/', '\\', '\0']) {
        return Err("名前に「/」や「\\」は使えません。".into());
    }
    Ok(name)
}

/// デバイスから受け取った名前が、ローカルの 1 階層分のファイル名として安全なら true。
///
/// 名前はデバイスが自由に申告できるので、`../../.bashrc` のような値で
/// 保存先の外へ書き込まれないよう、そのままパスに連結してはいけない。
pub fn is_safe_local_name(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains(['/', '\0'])
}

pub fn format_size(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

pub fn format_datetime(value: Option<DateTime>) -> String {
    value.map_or_else(String::new, |d| {
        format!(
            "{:04}-{:02}-{:02} {:02}:{:02}",
            d.year, d.month, d.day, d.hour, d.minute
        )
    })
}

/// リネーム時に拡張子を除いた部分だけを選択するための、終端バイト位置。
pub fn stem_end(name: &str, is_folder: bool) -> usize {
    if is_folder {
        return name.len();
    }
    match name.rfind('.') {
        Some(0) | None => name.len(),
        Some(dot) => dot,
    }
}

/// 長くなりすぎないよう、先頭の数件だけを改行区切りで並べる。
pub fn summarize_names(names: &[String]) -> String {
    const SHOWN: usize = 5;
    let mut text = names
        .iter()
        .take(SHOWN)
        .map(|n| format!("・{n}"))
        .collect::<Vec<_>>()
        .join("\n");
    if names.len() > SHOWN {
        text.push_str(&format!("\n他 {} 件", names.len() - SHOWN));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_name_is_trimmed_and_rejects_path_separators() {
        assert_eq!(validate_new_name("  走行記録.fit "), Ok("走行記録.fit"));
        for bad in ["", "   ", ".", "..", "a/b", "a\\b"] {
            assert!(validate_new_name(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn local_name_rejects_traversal() {
        assert!(is_safe_local_name("日本語.fit"));
        assert!(is_safe_local_name("..hidden"));
        for bad in ["", ".", "..", "../x", "a/b", "/etc/passwd", "a\0b"] {
            assert!(!is_safe_local_name(bad), "{bad:?}");
        }
    }

    #[test]
    fn sizes_are_human_readable() {
        assert_eq!(format_size(0), "0 B");
        assert_eq!(format_size(1023), "1023 B");
        assert_eq!(format_size(1536), "1.5 KB");
        assert_eq!(format_size(5 * 1024 * 1024 * 1024), "5.0 GB");
    }

    #[test]
    fn stem_excludes_extension_on_utf8_boundary() {
        assert_eq!(stem_end("走行.fit", false), "走行".len());
        assert_eq!(stem_end(".hidden", false), ".hidden".len());
        assert_eq!(stem_end("noext", false), 5);
        assert_eq!(stem_end("a.folder", true), 8);
    }

    #[test]
    fn long_name_lists_are_truncated() {
        let names: Vec<String> = (0..7).map(|i| i.to_string()).collect();
        let text = summarize_names(&names);
        assert!(text.ends_with("他 2 件"));
        assert_eq!(text.lines().count(), 6);
    }
}
