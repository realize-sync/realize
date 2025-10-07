use realize_storage::{FileAlternative, Version};

/// Format a list of FileAlternatives as a single string with
/// newline-separated entries as output for the xattr
/// realize.versions.
///
/// Each line represents one alternative, ending with a newline.
/// Empty input produces empty string.
pub(crate) fn format_versions(alts: &[FileAlternative]) -> String {
    if alts.is_empty() {
        return String::new();
    }

    let mut result = String::new();
    for alt in alts {
        result.push_str(&format_alternative(alt));
        result.push('\n');
    }

    result
}

/// Format a single FileAlternative for display.
fn format_alternative(alt: &FileAlternative) -> String {
    match alt {
        FileAlternative::Local(version) => match version {
            Version::Modified(Some(hash)) => format!("local {}", hash),
            Version::Modified(None) => "local modified".to_string(),
            Version::Indexed(hash) => format!("local {}", hash),
        },
        FileAlternative::Branched(path, hash) => {
            format!("branched {} {}", path, hash)
        }
        FileAlternative::Remote(peer, hash, size, mtime) => {
            format!("{} {} {} {}", peer, hash, size, mtime.display())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use realize_types::{Hash, Path, Peer, UnixTime};

    #[test]
    fn test_format_local_indexed() {
        let hash = Hash([1; 32]);
        let alt = FileAlternative::Local(Version::Indexed(hash));
        assert_eq!(
            format_alternative(&alt),
            "local AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE"
        );
    }

    #[test]
    fn test_format_local_modified_with_hash() {
        let hash = Hash([2; 32]);
        let alt = FileAlternative::Local(Version::Modified(Some(hash)));
        assert_eq!(
            format_alternative(&alt),
            "local AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI"
        );
    }

    #[test]
    fn test_format_local_modified_no_hash() {
        let alt = FileAlternative::Local(Version::Modified(None));
        assert_eq!(format_alternative(&alt), "local modified");
    }

    #[test]
    fn test_format_branched() {
        let path = Path::parse("some/path").unwrap();
        let hash = Hash([3; 32]);
        let alt = FileAlternative::Branched(path, hash);
        assert_eq!(
            format_alternative(&alt),
            "branched some/path AwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwMDAwM"
        );
    }

    #[test]
    fn test_format_remote() {
        let peer = Peer::from("peer1");
        let hash = Hash([4; 32]);
        let size = 100;
        let mtime = UnixTime::new(1234567890, 333999111); // 2009-02-13T23:31:30.333
        let expected = format!("peer1 {} 100 2009-02-13T23:31:30.333", hash);
        let alt = FileAlternative::Remote(peer, hash, size, mtime);
        assert_eq!(format_alternative(&alt), expected);
    }

    #[test]
    fn test_format_versions_empty() {
        assert_eq!(format_versions(&[]), "");
    }

    #[test]
    fn test_format_versions_single() {
        let hash = Hash([1; 32]);
        let alt = FileAlternative::Local(Version::Indexed(hash));
        assert_eq!(
            format_versions(&[alt]),
            "local AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE\n"
        );
    }

    #[test]
    fn test_format_versions_multiple() {
        let local_alt = FileAlternative::Local(Version::Indexed(Hash([1; 32])));
        let remote_alt = FileAlternative::Remote(
            Peer::from("peer1"),
            Hash([2; 32]),
            200,
            UnixTime::new(1640995200, 0), // 2022-01-01T00:00:00.000
        );

        let result = format_versions(&[local_alt, remote_alt]);
        let expected = "local AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE\npeer1 AgICAgICAgICAgICAgICAgICAgICAgICAgICAgICAgI 200 2022-01-01T00:00:00.000\n";
        assert_eq!(result, expected);
    }
}
