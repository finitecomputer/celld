// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! fork: an image's build context as a tar, for a builder that is not on
//! this machine (`krun_build`: BuildKit in a microVM of the krun engine).
//! `docker build` sends its daemon the context less what `.dockerignore`
//! leaves out; a builder in a VM gets it the same way, as a tar streamed
//! into the VM.
//!
//! The rules are BuildKit's own client's: moby/patternmatcher v0.6.1 (its
//! `ignorefile` and `MatchesUsingParentResults`) as fsutil's walk applies
//! them, so the tar holds exactly what `docker build` would send, no
//! excluded file entering the VM. Patterns go in order, the last match
//! deciding, `!` taking a path back; a path below a directory reuses what
//! each pattern made of that directory; a directory left out is skipped
//! whole as fsutil skips it (`prunes`).
//!
//! Bespoke because no maintained crate implements these rules (`glob` and
//! the gitignore matchers anchor and treat `**` differently), and the tar
//! is the `tar` crate's.

// The build context is the project's files, read as `docker build` reads
// them: outside the node's storage and its execution boundary.
#![allow(clippy::disallowed_methods)]

use anyhow::{bail, Context};
use std::path::{Path, PathBuf};

/// Files and directories one context may hold, at most.
pub const ENTRIES_MAX: usize = 200_000;
/// Bytes of file content one context may hold, at most: a builder VM's
/// root disk holds the context while it builds.
pub const BYTES_MAX: u64 = 4 << 30;
/// Patterns one ignore file may hold, at most.
const PATTERNS_MAX: usize = 4096;
/// A directory nested this deep is refused rather than walked.
const DEPTH_MAX: usize = 128;

/// How a pattern matches, as patternmatcher compiles it: its fast paths
/// are not quite its regular expression (`**.go` matches `a.go` as a
/// suffix), so both are kept.
#[derive(Debug, Clone)]
enum Kind {
    Exact(String),
    Prefix(String),
    Suffix(String),
    Regex(regex::Regex),
}

#[derive(Debug, Clone)]
struct Pattern {
    /// The pattern, cleaned, without its `!`.
    text: String,
    exclusion: bool,
    kind: Kind,
}

/// Escaped where patternmatcher escapes them: characters a regular
/// expression reads and `filepath.Match` does not.
fn should_escape(c: char) -> bool {
    matches!(c, '.' | '+' | '(' | ')' | '|' | '{' | '}' | '$')
}

/// patternmatcher's `filepath.Clean` of a slash-separated pattern: `.` and
/// empty parts dropped, `..` taking its parent away.
fn clean(path: &str) -> String {
    let rooted = path.starts_with('/');
    let mut parts: Vec<&str> = Vec::new();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                if parts.last().is_some_and(|p| *p != "..") {
                    parts.pop();
                } else if !rooted {
                    parts.push("..");
                }
            }
            p => parts.push(p),
        }
    }
    let joined = parts.join("/");
    match (rooted, joined.is_empty()) {
        (true, _) => format!("/{joined}"),
        (false, true) => ".".to_string(),
        (false, false) => joined,
    }
}

impl Pattern {
    fn new(raw: &str) -> anyhow::Result<Option<Self>> {
        let raw = raw.trim();
        if raw.is_empty() {
            return Ok(None);
        }
        let (exclusion, rest) = match raw.strip_prefix('!') {
            Some(rest) => (true, rest),
            None => (false, raw),
        };
        let text = clean(rest);
        if text.is_empty() || (exclusion && rest.trim().is_empty()) {
            bail!("illegal exclusion pattern: {raw:?}");
        }
        let kind = Self::compile(&text).with_context(|| format!("the ignore pattern {raw:?}"))?;
        Ok(Some(Pattern {
            text,
            exclusion,
            kind,
        }))
    }

    /// patternmatcher's `compile`, slash-separated.
    fn compile(pattern: &str) -> anyhow::Result<Kind> {
        #[derive(PartialEq)]
        enum M {
            Exact,
            Prefix,
            Suffix,
            Regex,
        }
        let mut m = M::Exact;
        let mut re = String::from("^");
        // Inside a bracket, Rust's regex reads `&&`, `--` and `~~` as set
        // operations, which Go's (patternmatcher's) does not: there `&` and
        // `~` are themselves.
        let mut in_class = false;
        let chars: Vec<char> = pattern.chars().collect();
        let mut i = 0;
        // Bounded by the pattern's length.
        while i < chars.len() {
            let c = chars[i];
            let first = i == 0;
            i += 1;
            match c {
                '*' if chars.get(i) == Some(&'*') => {
                    i += 1;
                    if chars.get(i) == Some(&'/') {
                        i += 1;
                    }
                    if i == chars.len() {
                        if m == M::Exact {
                            m = M::Prefix;
                        } else {
                            re.push_str(".*");
                            m = M::Regex;
                        }
                    } else {
                        re.push_str("(.*/)?");
                        m = M::Regex;
                    }
                    if first {
                        m = M::Suffix;
                    }
                }
                '*' => {
                    re.push_str("[^/]*");
                    m = M::Regex;
                }
                '?' => {
                    re.push_str("[^/]");
                    m = M::Regex;
                }
                c if should_escape(c) => {
                    re.push('\\');
                    re.push(c);
                }
                // As Go passes it: `\s` is whitespace, `\.` a dot.
                '\\' => match chars.get(i) {
                    Some(next) => {
                        // Go's Perl classes are ASCII; Rust's, Unicode.
                        match (*next, in_class) {
                            ('s', true) => re.push_str("\\t\\n\\x0C\\r "),
                            ('d', true) => re.push_str("0-9"),
                            ('w', true) => re.push_str("0-9A-Za-z_"),
                            ('s', false) => re.push_str("[\\t\\n\\x0C\\r ]"),
                            ('S', false) => re.push_str("[^\\t\\n\\x0C\\r ]"),
                            ('d', false) => re.push_str("[0-9]"),
                            ('D', false) => re.push_str("[^0-9]"),
                            ('w', false) => re.push_str("[0-9A-Za-z_]"),
                            ('W', false) => re.push_str("[^0-9A-Za-z_]"),
                            (next, _) => {
                                re.push('\\');
                                re.push(next);
                            }
                        }
                        i += 1;
                        m = M::Regex;
                    }
                    None => re.push_str("\\\\"),
                },
                '[' if in_class && chars.get(i) == Some(&':') => {
                    // A POSIX class (`[:digit:]`), the same in both, as is.
                    let end = chars[i..].windows(2).position(|w| w == [':', ']']).map(|e| i + e + 2);
                    match end {
                        Some(end) => {
                            re.push('[');
                            re.extend(&chars[i..end]);
                            i = end;
                        }
                        None => re.push_str("\\["),
                    }
                }
                '[' | ']' => {
                    // Any other `[` inside a bracket is itself (Go has no
                    // nested classes).
                    if c == '[' && in_class {
                        re.push_str("\\[");
                    } else {
                        in_class = c == '[';
                        re.push(c);
                    }
                    m = M::Regex;
                }
                '&' | '~' if in_class => {
                    re.push('\\');
                    re.push(c);
                }
                // Other characters a regular expression reads (`^`, a
                // bracket's `-`) pass as patternmatcher passes them.
                c => re.push(c),
            }
        }
        Ok(match m {
            M::Exact => Kind::Exact(pattern.to_string()),
            M::Prefix => Kind::Prefix(pattern.trim_end_matches('*').to_string()),
            M::Suffix => Kind::Suffix(pattern[2..].to_string()),
            M::Regex => {
                re.push('$');
                Kind::Regex(regex::Regex::new(&re).context("compile")?)
            }
        })
    }

    fn matches(&self, path: &str) -> bool {
        match &self.kind {
            Kind::Exact(p) => path == p,
            Kind::Prefix(p) => path.starts_with(p.as_str()),
            Kind::Suffix(s) => path.ends_with(s.as_str()) || s.strip_prefix('/').is_some_and(|s| path == s),
            Kind::Regex(re) => re.is_match(path),
        }
    }

    /// fsutil's `patternWithoutTrailingGlob`: the pattern less a trailing
    /// `/**` or `/*`.
    fn without_trailing_glob(&self) -> &str {
        let t = self.text.strip_suffix("/**").unwrap_or(&self.text);
        t.strip_suffix("/*").unwrap_or(t)
    }
}

/// What each pattern made of a directory (patternmatcher's `MatchInfo`):
/// whether it matched, `false` for one passed over.
pub type MatchInfo = Vec<bool>;

/// A `.dockerignore`, compiled.
#[derive(Debug, Clone, Default)]
pub struct Ignore {
    patterns: Vec<Pattern>,
}

impl Ignore {
    /// `ignorefile.ReadAll`: a line starting with `#` is a comment, a UTF-8
    /// byte order mark leads the first line, and a pattern is relative to
    /// the context whether or not it starts with `/`.
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let text = text.strip_prefix('\u{feff}').unwrap_or(text);
        let mut patterns = Vec::new();
        for line in text.lines() {
            if line.starts_with('#') {
                continue;
            }
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let (invert, body) = match line.strip_prefix('!') {
                Some(rest) => (true, rest.trim()),
                None => (false, line),
            };
            let mut body = if body.is_empty() {
                String::new()
            } else {
                clean(body)
            };
            if body.len() > 1 && body.starts_with('/') {
                body.remove(0);
            }
            let line = if invert { format!("!{body}") } else { body };
            if let Some(pattern) = Pattern::new(&line)? {
                patterns.push(pattern);
                anyhow::ensure!(patterns.len() <= PATTERNS_MAX, "an ignore file of more than {PATTERNS_MAX} patterns");
            }
        }
        Ok(Self { patterns })
    }

    /// patternmatcher's `MatchesOrParentMatches`: whether `path` (relative,
    /// slash-separated) is left out.
    #[cfg(test)]
    pub fn excludes(&self, path: &str) -> bool {
        let parents: Vec<&str> = match path.rfind('/') {
            Some(i) => path[..i].split('/').collect(),
            None => Vec::new(),
        };
        let mut matched = false;
        for pattern in &self.patterns {
            // An inclusion when already matched, or an exclusion when not,
            // cannot change the answer.
            if pattern.exclusion != matched {
                continue;
            }
            let mut hit = pattern.matches(path);
            if !hit {
                for i in 0..parents.len() {
                    if pattern.matches(&parents[..=i].join("/")) {
                        hit = true;
                        break;
                    }
                }
            }
            if hit {
                matched = !pattern.exclusion;
            }
        }
        matched
    }

    /// patternmatcher's `MatchesUsingParentResults`: whether `path` is left
    /// out, given what each pattern made of its directory (`parent`; `None`
    /// at the context's top), and what each made of `path`, for below it.
    pub fn matches_using_parent(&self, path: &str, parent: Option<&MatchInfo>) -> (bool, MatchInfo) {
        let mut matched = false;
        let mut info = vec![false; self.patterns.len()];
        for (i, pattern) in self.patterns.iter().enumerate() {
            let mut hit = parent.is_some_and(|p| p[i]);
            if !hit {
                // An inclusion when already matched, or an exclusion when
                // not, is passed over, and recorded as no match.
                if pattern.exclusion != matched {
                    continue;
                }
                hit = pattern.matches(path);
                if !hit && parent.is_none() {
                    if let Some((dir, _)) = path.rsplit_once('/') {
                        let dirs: Vec<&str> = dir.split('/').collect();
                        hit = (0..dirs.len()).any(|j| pattern.matches(&dirs[..=j].join("/")));
                    }
                }
            }
            info[i] = hit;
            if hit {
                matched = !pattern.exclusion;
            }
        }
        (matched, info)
    }

    /// fsutil's walk: a directory left out is skipped whole unless a `!`
    /// pattern below it names it by its path (a `!` pattern with a glob
    /// short of its last part has every directory walked).
    fn prunes(&self, dir: &str) -> bool {
        let exclusions: Vec<&Pattern> = self.patterns.iter().filter(|p| p.exclusion).collect();
        let prefix_only = !exclusions.iter().any(|p| p.without_trailing_glob().contains(['*', '[', ']', '?', '^', '\\']));
        if !prefix_only {
            return false;
        }
        let below = format!("{dir}/");
        !exclusions.iter().any(|p| format!("{}/", p.without_trailing_glob()).starts_with(&below))
    }
}

/// The ignore file BuildKit reads for `dockerfile`'s build: one beside it
/// named for it (`<Dockerfile>.dockerignore`), or the context's own.
pub fn ignore_file(dockerfile: &Path, context: &Path) -> Option<PathBuf> {
    let mut own = dockerfile.as_os_str().to_owned();
    own.push(".dockerignore");
    let own = PathBuf::from(own);
    if own.is_file() {
        return Some(own);
    }
    let shared = context.join(".dockerignore");
    shared.is_file().then_some(shared)
}

/// What `write` put in a tar.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Written {
    pub entries: usize,
    pub bytes: u64,
}

/// Appends `context` to `tar` under `prefix`, less what `ignore` leaves
/// out: directories, regular files and symbolic links (kept as links, as
/// Docker sends them), with their modes and times. Sockets, devices and
/// pipes are left, as BuildKit's client leaves them.
pub fn write<W: std::io::Write>(
    tar: &mut tar::Builder<W>,
    context: &Path,
    prefix: &str,
    ignore: &Ignore,
) -> anyhow::Result<Written> {
    tar.follow_symlinks(false);
    let mut written = Written::default();
    tar.append_dir(prefix, context)
        .with_context(|| format!("add {} to the build context", context.display()))?;
    written.entries += 1;
    let mut stack: Vec<(PathBuf, String, usize, Option<MatchInfo>)> = vec![(context.to_path_buf(), String::new(), 0, None)];
    // Bounded by ENTRIES_MAX and DEPTH_MAX.
    while let Some((dir, rel, depth, parent)) = stack.pop() {
        anyhow::ensure!(depth <= DEPTH_MAX, "the build context nests past {DEPTH_MAX} directories at {}", dir.display());
        let mut entries: Vec<_> = std::fs::read_dir(&dir)
            .with_context(|| format!("read {}", dir.display()))?
            .collect::<Result<_, _>>()
            .with_context(|| format!("read {}", dir.display()))?;
        // The tar is the same for the same files.
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                bail!("{} is not UTF-8, which an ignore pattern cannot name", entry.path().display());
            };
            let path = if rel.is_empty() {
                name.to_string()
            } else {
                format!("{rel}/{name}")
            };
            let kind = entry.file_type().with_context(|| format!("stat {}", entry.path().display()))?;
            let (excluded, info) = ignore.matches_using_parent(&path, parent.as_ref());
            if kind.is_dir() {
                if excluded && ignore.prunes(&path) {
                    continue;
                }
                // Kept even when left out, for what is taken back below it:
                // an empty directory more is all BuildKit's filter drops.
                tar.append_dir(format!("{prefix}/{path}"), entry.path())
                    .with_context(|| format!("add {}", entry.path().display()))?;
                written.entries += 1;
                stack.push((entry.path(), path, depth + 1, Some(info)));
            } else if (kind.is_file() || kind.is_symlink()) && !excluded {
                if kind.is_file() {
                    let len = entry.metadata().with_context(|| format!("stat {}", entry.path().display()))?.len();
                    written.bytes += len;
                    anyhow::ensure!(written.bytes <= BYTES_MAX, "the build context {} is past {BYTES_MAX} bytes; leave more out with .dockerignore", context.display());
                }
                tar.append_path_with_name(entry.path(), format!("{prefix}/{path}"))
                    .with_context(|| format!("add {}", entry.path().display()))?;
                written.entries += 1;
            }
            anyhow::ensure!(written.entries <= ENTRIES_MAX, "the build context {} holds more than {ENTRIES_MAX} entries; leave more out with .dockerignore", context.display());
        }
    }
    Ok(written)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ignore(text: &str) -> Ignore {
        Ignore::parse(text).unwrap()
    }

    // Goal: the rules are Docker's, as patternmatcher's own tests state them.
    #[test]
    fn patterns_match_as_docker_matches() {
        let cases: &[(&str, &str, bool)] = &[
            ("*", "a", true),
            ("*", "a/b", true),
            ("a", "a/b/c", true),
            ("*.go", "a.go", true),
            ("*.go", "d/a.go", false),
            ("**/*.go", "d/a.go", true),
            ("**/*.go", "a.go", true),
            ("**.go", "x/a.go", true),
            ("**", "a/b", true),
            ("a/**", "a/b/c", true),
            ("a/**", "b/a", false),
            ("**/target", "target", true),
            ("**/target", "crates/x/target", true),
            ("**/target", "crates/x/target/debug/a", true),
            ("**/target", "crates/targets", false),
            ("a?c", "abc", true),
            ("a?c", "a/c", false),
            ("[ab]x", "bx", true),
            ("[^a]x", "ax", false),
            ("/abs", "abs", true),
            ("./rel/../dir", "dir/x", true),
            ("a.b", "axb", false),
            ("\\*", "*", true),
            ("\\*", "a", false),
            // Go's brackets have no set operations: `&` is itself
            ("[a&&b].env", "a.env", true),
            ("[a&&b].env", "&.env", true),
            ("[^a&&b].env", "a.env", false),
            ("[^a&&b].env", "c.env", true),
            ("[a~~b]x", "~x", true),
            ("[a-c]x", "bx", true),
            ("local[[:digit:]].env", "local1.env", true),
            ("local[[:digit:]].env", "locala.env", false),
            ("[a[b]x", "[x", true),
            ("secrets\\s*.env", "secrets prod.env", true),
            ("secrets\\s*.env", "secretss.env", false),
            ("secrets\\s*.env", "secrets\u{a0}prod.env", false),
            ("a[\\s]b", "a b", true),
            ("a[\\d]b", "a1b", true),
            ("a\\Sb", "a\u{e9}b", true),
            ("a\\Sb", "a b", false),
            ("a\\db", "a\u{0663}b", false),
            ("a\\.b", "a.b", true),
        ];
        for (pattern, path, want) in cases {
            assert_eq!(ignore(pattern).excludes(path), *want, "{pattern:?} on {path:?}");
        }
    }

    // Goal: the last pattern that matches decides, and `!` takes paths
    // back, parents included.
    #[test]
    fn exclusions_take_paths_back_in_order() {
        let i = ignore("# a comment\n*\n!Cargo.toml\n!crates\n**/target\n\n");
        assert!(i.excludes("README.md"));
        assert!(!i.excludes("Cargo.toml"));
        assert!(!i.excludes("crates/a/src/lib.rs"));
        assert!(i.excludes("crates/a/target/debug/x"));
        let later = ignore("**/target\n!crates");
        assert!(!later.excludes("crates/a/target/x"), "a later ! of a parent takes it back");
        assert!(ignore("!x\n*").excludes("x"), "an earlier ! is undone");
        assert!(Ignore::parse("!").is_err());
        assert!(Ignore::parse("[").is_err());
    }

    // Goal: a directory is skipped as fsutil skips it.
    #[test]
    fn a_directory_is_pruned_as_fsutil_prunes_it() {
        let i = ignore("*\n!Cargo.toml\n!cli\n**/target\n");
        assert!(i.excludes("target") && i.prunes("target"));
        assert!(i.excludes("cli/target") && i.prunes("cli/target"));
        let deep = ignore("*\n!docs/keep.md\n");
        assert!(deep.excludes("docs") && !deep.prunes("docs"));
        assert!(deep.prunes("src"));
        assert!(!ignore("*\n!docs/**\n").prunes("docs"), "a trailing glob is a prefix");
        let glob = ignore("*\n!**/keep\n");
        assert!(!glob.prunes("anything"), "a glob in a ! pattern walks every directory");
        assert!(ignore("*\n").prunes("x"), "no ! pattern at all");
    }

    // Goal: below a directory, each pattern is what it was for the
    // directory, and one passed over there stays no match: `!src` after
    // `src/.env` takes `src` back, never `src/.env`.
    #[test]
    fn a_path_below_reuses_its_directorys_matches() {
        let i = ignore("src/.env\n!src\n");
        let (src, info) = i.matches_using_parent("src", None);
        assert!(!src);
        assert!(i.matches_using_parent("src/.env", Some(&info)).0, "the secret stays out");
        assert!(!i.matches_using_parent("src/main.rs", Some(&info)).0);
        let j = ignore("*\n!**/keep\n");
        let (a, info) = j.matches_using_parent("a", None);
        assert!(a);
        assert!(!j.matches_using_parent("a/keep", Some(&info)).0, "taken back below an excluded directory");
        assert!(j.matches_using_parent("a/other", Some(&info)).0);
    }

    fn listed(dir: &Path, ignore_text: &str) -> Vec<String> {
        let mut tar = tar::Builder::new(Vec::new());
        write(&mut tar, dir, "context", &Ignore::parse(ignore_text).unwrap()).unwrap();
        let bytes = tar.into_inner().unwrap();
        let mut names: Vec<String> = tar::Archive::new(&bytes[..])
            .entries()
            .unwrap()
            .map(|e| e.unwrap().path().unwrap().to_string_lossy().trim_end_matches('/').to_string())
            .collect();
        names.sort();
        names
    }

    // Goal: the tar holds what Docker would send, links kept as links.
    #[test]
    fn the_tar_holds_the_context_less_what_is_left_out() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        for f in ["Cargo.toml", "README.md", "cli/src/main.rs", "cli/target/debug/big", "docs/keep.md", "docs/other.md"] {
            std::fs::create_dir_all(root.join(f).parent().unwrap()).unwrap();
            std::fs::write(root.join(f), f).unwrap();
        }
        std::os::unix::fs::symlink("main.rs", root.join("cli/src/link.rs")).unwrap();
        std::fs::write(root.join("cli/.env"), "SECRET").unwrap();
        let names = listed(root, "*\n!Cargo.toml\n!cli\ncli/.env\n!docs/keep.md\n**/target\n");
        assert_eq!(
            names,
            ["context", "context/Cargo.toml", "context/cli", "context/cli/src", "context/cli/src/link.rs", "context/cli/src/main.rs", "context/docs", "context/docs/keep.md"]
        );
        let mut tar = tar::Builder::new(Vec::new());
        write(&mut tar, root, "context", &Ignore::default()).unwrap();
        let bytes = tar.into_inner().unwrap();
        let mut archive = tar::Archive::new(&bytes[..]);
        let link = archive
            .entries()
            .unwrap()
            .map(Result::unwrap)
            .find(|e| e.path().unwrap().ends_with("link.rs"))
            .unwrap();
        assert!(link.header().entry_type().is_symlink());
    }

    #[test]
    fn the_ignore_file_is_the_dockerfiles_own_first() {
        let dir = tempfile::tempdir().unwrap();
        let df = dir.path().join("images/x.Dockerfile");
        std::fs::create_dir_all(df.parent().unwrap()).unwrap();
        std::fs::write(&df, "FROM scratch").unwrap();
        assert_eq!(ignore_file(&df, dir.path()), None);
        std::fs::write(dir.path().join(".dockerignore"), "*").unwrap();
        assert_eq!(ignore_file(&df, dir.path()), Some(dir.path().join(".dockerignore")));
        std::fs::write(dir.path().join("images/x.Dockerfile.dockerignore"), "*").unwrap();
        assert_eq!(ignore_file(&df, dir.path()), Some(dir.path().join("images/x.Dockerfile.dockerignore")));
    }
}
