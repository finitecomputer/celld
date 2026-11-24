//! fork: a `durable_objects` binding whose `script_name` names another
//! script reaches that script's objects, as on Cloudflare: the binding
//! script hosts none of them and never runs their class's code, and its
//! calls reach the cells the owner's own binding reaches. A binding to a
//! script outside the deployment, or to a class that script does not
//! declare, stops the node before it serves.

#![cfg(unix)]
// A test of the binary drives real processes, sockets, files, and clocks.
#![allow(clippy::disallowed_methods)]

mod support;

use std::ffi::{OsStr, OsString};
use std::path::Path;
use support::Dev;

/// The owner: two classes, and a service binding to the binder, which is
/// how the node's deployment graph reaches the binder (as a cell reaches
/// its admin Worker).
const OWNER: &str = r#"
import { DurableObject } from "cloudflare:workers";

const seen = async (ctx) => {
  const count = ((await ctx.storage.get("count")) ?? 0) + 1;
  await ctx.storage.put("count", count);
  return { by: "owner", count, id: ctx.id.toString(), name: ctx.id.name ?? null };
};

export class Counter extends DurableObject {
  async fetch() {
    return Response.json(await seen(this.ctx));
  }
  async count() {
    return seen(this.ctx);
  }
}

export class Other extends DurableObject {
  async fetch() {
    return Response.json({ ...(await seen(this.ctx)), class: "Other" });
  }
}

export default {
  async fetch(request, env) {
    const [, route, arg] = new URL(request.url).pathname.split("/");
    switch (route) {
      case "own":
        return env.COUNTER.getByName(arg).fetch(request);
      case "unique": {
        const id = env.COUNTER.newUniqueId();
        const seen = await (await env.COUNTER.get(id).fetch(request)).json();
        return Response.json({ ...seen, minted: id.toString() });
      }
      default:
        return env.BINDER.fetch(request);
    }
  },
};
"#;

const OWNER_CONFIG: &str = r#"{
  "name": "owner",
  "main": "index.js",
  "compatibility_date": "2026-01-01",
  "no_bundle": true,
  "durable_objects": { "bindings": [
    { "name": "COUNTER", "class_name": "Counter" },
    { "name": "OTHER", "class_name": "Other" }
  ] },
  "migrations": [{ "tag": "v1", "new_sqlite_classes": ["Counter", "Other"] }],
  "services": [{ "binding": "BINDER", "service": "binder" }]
}"#;

/// The binder: the owner's two classes by `script_name`. It exports a
/// `Counter` of its own, which must never run: its constructor throws.
const BINDER: &str = r#"
import { DurableObject } from "cloudflare:workers";

export class Counter extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    throw new Error("the binder's own Counter ran");
  }
}

export default {
  async fetch(request, env) {
    const [, route, arg] = new URL(request.url).pathname.split("/");
    switch (route) {
      case "via":
        return env.COUNTER.getByName(arg).fetch(request);
      case "via-id":
        return env.COUNTER.get(env.COUNTER.idFromString(arg)).fetch(request);
      case "rpc":
        return Response.json(await env.COUNTER.getByName(arg).count());
      case "other":
        return env.OTHER.getByName(arg).fetch(request);
      case "names":
        return Response.json({ COUNTER: env.COUNTER.constructor.name, OTHER: env.OTHER.constructor.name });
    }
    return new Response("where?", { status: 404 });
  },
};
"#;

fn binder_config(bindings: &str) -> String {
    format!(
        r#"{{
  "name": "binder",
  "main": "index.js",
  "compatibility_date": "2026-01-01",
  "no_bundle": true,
  "durable_objects": {{ "bindings": [{bindings}] }}
}}"#
    )
}

const BINDS_BOTH: &str = r#"
    { "name": "COUNTER", "class_name": "Counter", "script_name": "owner" },
    { "name": "OTHER", "class_name": "Other", "script_name": "owner" }
"#;

fn write(dir: &Path, config: &str, worker: &str) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(dir.join("wrangler.jsonc"), config).unwrap();
    std::fs::write(dir.join("index.js"), worker).unwrap();
}

/// `celld dev OWNER --with BINDER`, the owner the application.
fn owner_with_binder(root: &Path) -> Vec<OsString> {
    vec![root.join("owner").into(), "--with".into(), root.join("binder").into()]
}

// Goal: the binder's calls reach the owner's objects by name, by an id the
// owner minted, and by RPC: one count, one id, run by the owner's class, the
// binder's own `Counter` never constructed. Its bindings are workerd's
// class. Replay: asked again, the count goes on. Restart: a node started
// again on the same state reads the count where it stopped.
#[test]
fn a_binding_reaches_the_owning_scripts_objects() {
    let root = tempfile::tempdir().unwrap();
    write(&root.path().join("owner"), OWNER_CONFIG, OWNER);
    write(&root.path().join("binder"), &binder_config(BINDS_BOTH), BINDER);
    let args = owner_with_binder(root.path());
    let id = {
        let dev = Dev::start_args(&args, &[]);
        assert_eq!(
            dev.json("/names").0,
            serde_json::json!({ "COUNTER": "DurableObjectNamespace", "OTHER": "DurableObjectNamespace" })
        );
        let (own, _) = dev.json("/own/a");
        assert_eq!((own["by"].as_str(), own["count"].as_u64(), own["name"].as_str()), (Some("owner"), Some(1), Some("a")), "{own}");
        let id = own["id"].as_str().unwrap().to_string();
        let mut expected = 1;
        for path in ["/via/a", "/rpc/a", "/own/a", "/via/a"] {
            expected += 1;
            let (got, _) = dev.json(path);
            assert_eq!(got["by"], "owner", "{path}: {got}\n{}", dev.output());
            assert_eq!(got["count"], expected, "{path}: one object, one count: {got}");
            assert_eq!(got["id"], id.as_str(), "{path}: the owner's id: {got}");
        }

        let (minted, _) = dev.json("/unique/x");
        assert_eq!(minted["count"], 1, "{minted}");
        let unique = minted["minted"].as_str().unwrap();
        let (via, _) = dev.json(&format!("/via-id/{unique}"));
        assert_eq!((via["by"].as_str(), via["count"].as_u64(), via["id"].as_str()), (Some("owner"), Some(2), Some(unique)), "{via}");

        let (other, _) = dev.json("/other/b");
        assert_eq!((other["class"].as_str(), other["by"].as_str(), other["count"].as_u64()), (Some("Other"), Some("owner"), Some(1)), "{other}");

        assert!(!dev.output().contains("the binder's own Counter ran"), "{}", dev.output());
        id
    };

    let dev = Dev::start_args(&args, &[]);
    let (got, _) = dev.json("/via/a");
    assert_eq!((got["count"].as_u64(), got["id"].as_str()), (Some(6), Some(id.as_str())), "{got}");
    assert!(!dev.output().contains("the binder's own Counter ran"), "{}", dev.output());
}

// Goal: the owner's code runs the objects even when nothing but the object
// binding reaches it: the binder is the application, the owner only a
// script its bindings name.
#[test]
fn an_object_binding_loads_its_owner() {
    let root = tempfile::tempdir().unwrap();
    let owner = OWNER_CONFIG.replace(r#",
  "services": [{ "binding": "BINDER", "service": "binder" }]"#, "");
    write(&root.path().join("owner"), &owner, OWNER);
    write(&root.path().join("binder"), &binder_config(BINDS_BOTH), BINDER);
    let binder = root.path().join("binder");
    let owner = root.path().join("owner");
    let dev = Dev::start_args(&[binder.as_os_str(), OsStr::new("--with"), owner.as_os_str()], &[]);
    for expected in 1..=2 {
        let (got, _) = dev.json("/via/c");
        assert_eq!((got["by"].as_str(), got["count"].as_u64()), (Some("owner"), Some(expected)), "{got}");
    }
}

// Goal: a binding to a class its script does not declare, or to a script
// outside the deployment, stops the node before it serves, saying which.
#[test]
fn a_binding_to_no_such_class_or_script_is_refused() {
    for (bindings, says) in [
        (
            r#"{ "name": "COUNTER", "class_name": "Missing", "script_name": "owner" }"#,
            "script binder binds Durable Object class Missing of script owner, which declares no such class",
        ),
        (
            r#"{ "name": "COUNTER", "class_name": "Counter", "script_name": "nobody" }"#,
            "load Durable Object binding target nobody",
        ),
    ] {
        let root = tempfile::tempdir().unwrap();
        write(&root.path().join("owner"), OWNER_CONFIG, OWNER);
        write(&root.path().join("binder"), &binder_config(bindings), BINDER);
        let output = Dev::refused(&owner_with_binder(root.path()), &[]);
        assert!(output.contains(says), "{says:?} in:\n{output}");
    }
}
