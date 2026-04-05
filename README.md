# GPKG: GeminiOS Package Manager (new)

`gpkg-v2/` is the source tree for the current GeminiOS package manager
implementation.

Even though the implementation lives under `gpkg-v2/`, the built and installed
binaries are the canonical GeminiOS commands:

- `gpkg`
- `gpkg-worker`

This implementation handles repository metadata, dependency resolution,
downloads, and package operations across two backends:

- Debian packages are planned through vendored `apt`/`apt-private` logic and
  installed natively through `dpkg`
- GeminiOS-native `.gpkg` packages still use `gpkg-worker`

## License

`gpkg-v2/` follows the same GPL-2-or-later licensing model as the vendored
`apt` code it is built around.

- `LICENSE` states the project-wide `gpkg-v2` licensing terms
- `COPYING.GPL` carries the GNU GPL v2 text referenced by that metadata
- `vendor/apt/COPYING` and `vendor/apt/COPYING.GPL` preserve apt's upstream
  copyright and license metadata for the vendored code

Treat this tree as GPL-2-or-later for the project as a whole unless a more
specific file-level exception in `vendor/apt/COPYING` says otherwise.

## Current Role

- `gpkg-v2/` is the only package-manager source tree used by the full GeminiOS
  image build
- the legacy `gpkg/` binaries are no longer staged into `rootfs`
- the runtime CLI name is `gpkg`, not `gpkg-v2`

Small compatibility fallbacks for older `gpkg-v2` paths may still exist inside
the codebase, but they are only there to smooth transition and developer
testing. The intended installed interface is just `gpkg` and `gpkg-worker`.

## Layout

- `src/gpkg.cpp`: main CLI, repository handling, apt-backed dependency
  planning, and backend dispatch
- `src/gpkg_worker.cpp`: privileged worker for GeminiOS-native `.gpkg`
  extraction, registration, removal, and maintainer-script compatibility hooks
- `src/*.ipp`: shared package-manager implementation fragments
- `vendor/apt/`: vendored apt sources used by the Debian backend
- `Makefile`: standalone build/install entrypoint for this module

## Build

Build the canonical binaries from this tree:

```bash
cd gpkg-v2
make -j"$(nproc)"
```

This produces:

- `bin/gpkg`
- `bin/gpkg-worker`

Install into a target rootfs:

```bash
cd gpkg-v2
make -j"$(nproc)" install DESTDIR=/path/to/rootfs ROOTFS=/path/to/rootfs
```

The install step stages only the canonical filesystem paths:

- `/bin/apps/system/gpkg`
- `/bin/apps/system/gpkg-worker`
- `/bin/gpkg` -> `/bin/apps/system/gpkg`

It also removes stale `gpkg-v2` filesystem entries if they are present in the
destination rootfs.

## Full GeminiOS Integration

Within the full GeminiOS build, `ports/geminios_complex/build.sh` is the single
integration point for the package manager.

That build path now:

- compiles `gpkg` from `gpkg-v2/`
- installs only `gpkg` and `gpkg-worker` into `rootfs`
- does not build or stage the old `gpkg/` binaries into the image

## Runtime Notes

`gpkg` is designed to work in both live and normally installed GeminiOS
systems.

- On normal installed systems, package-manager state lives on the regular
  filesystem and low-space diagnostics refer to package-manager cache/state in a
  generic way.
- On live boots, GeminiOS now provisions dedicated writable package-manager
  state for `/var/repo` and `/var/lib/gpkg`, and `gpkg` reports live-specific
  storage guidance when that environment is detected.

That means this implementation is intended to be robust for:

- normal non-live installed systems
- live VM sessions with the updated GeminiOS boot path
- mixed Debian + `.gpkg` package environments
