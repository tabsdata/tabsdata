//
// Copyright 2024 Tabs Data Inc.
//

use url::Url;

pub trait AbsolutePath {
    /// Returns the absolute path of a location.
    fn abs_path(&self) -> String;
}

/// Returns the absolute path of a URL, for Unix systems.
///
/// This is equivalent [`Url::path`].
///
#[cfg(not(target_os = "windows"))]
impl AbsolutePath for Url {
    fn abs_path(&self) -> String {
        self.path().to_string()
    }
}

/// Returns the absolute path of a URL, for Windows systems.
///
/// This removes the leading `/` before the drive letter (unit), if it exists. I also normalizes `\` to `/`.
/// This replacement is necessary as Path.from assumes delimiter is /. We assume other pieces of code are able to
/// handle properly Windows paths even with slashes instead of back-slashes.
#[cfg(target_os = "windows")]
impl AbsolutePath for Url {
    fn abs_path(&self) -> String {
        let path = self.path();
        let normalized_path =
            if path.len() > 2 && path.as_bytes()[2] == b':' && path.starts_with('/') {
                &path[1..]
            } else {
                path
            };
        normalized_path.replace('\\', "/")
    }
}

#[cfg(test)]
mod tests {
    use crate::absolute_path::AbsolutePath;
    use url::Url;

    #[test]
    #[cfg(not(target_os = "windows"))]
    fn test_valid_path_in_unix() {
        //this is really weird: it is eating up the first path segment in Unix.
        assert_eq!(Url::parse("file://foo/").unwrap().abs_path(), "/");
        assert_eq!(Url::parse("file:///foo").unwrap().abs_path(), "/foo");
    }

    #[test]
    #[cfg(target_os = "windows")]
    fn test_valid_path_in_windows() {
        assert_eq!(Url::parse("file://foo/").unwrap().abs_path(), "/");
        assert_eq!(Url::parse("file:///foo").unwrap().abs_path(), "/foo");
        assert_eq!(Url::parse("file:///c:/foo").unwrap().abs_path(), "c:/foo");

        assert_eq!(Url::parse("file://foo\\").unwrap().abs_path(), "/");
        assert_eq!(Url::parse("file:///foo").unwrap().abs_path(), "/foo");
        assert_eq!(Url::parse("file:///c:\\foo").unwrap().abs_path(), "c:/foo");

        assert_eq!(Url::parse("file:///foo/lu").unwrap().abs_path(), "/foo/lu");
        assert_eq!(
            Url::parse("file:///c:/foo/lu").unwrap().abs_path(),
            "c:/foo/lu"
        );

        assert_eq!(Url::parse("file:///foo/lu").unwrap().abs_path(), "/foo/lu");
        assert_eq!(
            Url::parse("file:///c:\\foo\\lu").unwrap().abs_path(),
            "c:/foo/lu"
        );
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn test_abs_path_encoding() {
        let url = Url::parse("file:///c:\\one\\two\\three\\four.five").unwrap();
        assert_eq!(url.abs_path(), "c:/one/two/three/four.five");

        let url = Url::parse("file:///c:/one/two/three/four.five").unwrap();
        assert_eq!(url.abs_path(), "c:/one/two/three/four.five");
    }
}
