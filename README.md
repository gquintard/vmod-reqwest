# vmod_reqwest

This is a vmod for [varnish](http://varnish-cache.org/) to send and receive HTTP requests from VCL, leveraging the [reqwest crate](https://docs.rs/reqwest/latest/reqwest/).

It can be used in two different ways:
- to provide dynamic backends, similar to [vmod_dynamic](https://github.com/nigoroll/libvmod-dynamic) or [vmod_goto](https://docs.varnish-software.com/varnish-cache-plus/vmods/goto/)
  - Backend addresses are resolved when needed, not just when the VCL is loaded
  - support for backends with an address defined ahead of time, but also on-the-fly
  - probe support
  - ability to index the health of a backend on a probe targeting another
- as a mean to issue HTTP requests from VCL to communicate with third-party servers, much like [vmod_curl](https://github.com/varnish/libvmod-curl)
  - parallel requests
  - synchronous and asynchronous requests support

And in both cases:
- HTTP2 and HTTPS support
- HTTP redirection support (it will automatically follow 30X responses)
- (optional) automatic `gzip` and `brotli` decompression

As usual, the full VCL API is described in [vmod.vcc](vmod.vcc).

Don't hesitate to open github issues if something is unclear or impractical. You can also join us on [discord](https://discord.com/invite/EuwdvbZR6d).

## Version matching

| vmod-reqwest | varnish |
| :----------- | :-----: |
| 0.0.6        | 7.2     |
| 0.0.5        | 7.2     |
| 0.0.4        | 7.1     |
| 0.0.3        | 7.1     |
| 0.0.2        | 7.0     |
| 0.0.1        | 7.0     |

## VCL Examples

### VCL request: synchronous request and response headers
``` vcl
import reqwest;

sub vcl_init {
	new client = reqwest.client();
}

sub vcl_recv {
	# use an HTTP request to grant (or not) access to the client
	client.init("sync", "https://api.example.com/authorized/" + req.http.user);
	if (client.status("sync") == 200) {
		return (lookup);
	} else {
		return (synth(403));
	}
}
```

### VCL request: multiple requests in-flight at once

``` vcl
import reqwest;

sub vcl_init {
	new client = reqwest.client();
}

sub vcl_recv {
	# build and send a request to the legacy service
	client.init("req1", "https://login.example.com/user/" + req.http.user);
	client.send("req1");

	# same for the new endpoint
	client.init("req2", "https://loginv2.example.com/user/" + req.http.user);
	client.send("req2");

	# let the request through if at least one of the services
	# responded favorably
	if (client.status("req1") == 200 || client.status("req2") == 200) {
		return (lookup);
	} else {
		return (synth(403));
	}
}
```

### VCL request: Fire-and-forget request with body

``` vcl
import reqwest;

sub vcl_init {
	new client = reqwest.client();
}

sub vcl_recv {
	# send a request into the void and don't worry if it completes or not
	client.init("async", "https://api.example.com/log", "POST")
	client.set_body("async", "URL = " + req.url);
	client.send("async");
}
```

### Backend: HTTPS, following up to 5 redirect hops, and brotli auto-decompression

``` vcl
import reqwest;

sub vcl_init {
	# note that "/sub/directory" will be prefixed to `bereq.url`
	# upon sending the request to the backend
	new be = reqwest.client(base_url = "https://www.example.com/sub/directory", follow = 5, auto_brotli = true);
}

sub vcl_recv {
	set req.backend_hint = be.backend();
}
```

### Backend: Using a probe to one backend to determine another's health

``` vcl
import reqwest;

# create a probe to a specific endpoint
probe p1 {
	.url = "http://probed.example.com/probe";
	.window = 4;
	.initial = 2;
	.threshold = 4;
	.interval = 1s;
}

# attach the probe to a client
sub vcl_init {
	new be = reqwest.client(probe = p1);
}

# set the backend, which will use req.url+req.http.host as a destination,
# but only if probed.example.com is replying to probes
sub vcl_recv {
	set req.backend_hint = be.backend();
}
```

## Requirements

You'll need:
- `cargo` (and the accompanying `rust` package)
- `clang`
- `python3`
- the `varnish` 7.0.1 development libraries/headers ([depends on the `varnish` crate you are using](https://github.com/gquintard/varnish-rs#versions))

## Build and test

With `cargo` only:

``` bash
cargo build --release
cargo test --release
```

The vmod file will be found at `target/release/libvmod_reqwest.so`.

Alternatively, if you have `jq` and `rst2man`, you can use `build.sh`

``` bash
./build.sh [OUTDIR]
```

This will place the `so` file as well as the generated documentation in the `OUT` directory (or in the current directory if `OUT` wasn't specified).

## Packages

To avoid making a mess of your system, you probably should install your vmod as a proper package. This repository also offers different templates, and some quick recipes for different distributions.

### All platforms

First it's necessary to set the `VMOD_VERSION` (the version of this vmod) and `VARNISH_VERSION` (the Varnish version to build against) environment variables. It can be done manually, or using `cargo` and `jq`:
``` bash
VMOD_VERSION=$(cargo metadata --no-deps --format-version 1 | jq '.packages[0].version' -r)
VARNISH_MINOR=$(cargo metadata --format-version 1 | jq -r '.packages[] | select(.name == "varnish-sys") | .metadata.libvarnishapi.version ')
VARNISH_PATCH=0
VARNISH_VERSION="$VARNISH_MINOR.$VARNISH_PATCH"

# or
VMOD_VERSION=0.0.1
VARNISH_VERSION=7.0.0
```

Then create the dist tarball, for example using `git archive`:

``` bash
git archive --output=vmod_reqwest-$VMOD_VERSION.tar.gz --format=tar.gz HEAD
```

Then, follow distribution-specific instructions.

### Arch

``` bash
# create a work directory
mkdir build
# copy the tarball and PKGBUILD file, substituing the variables we care about
cp vmod_reqwest-$VMOD_VERSION.tar.gz build
sed -e "s/@VMOD_VERSION@/$VMOD_VERSION/" -e "s/@VARNISH_VERSION@/$VARNISH_VERSION/" pkg/arch/PKGBUILD > build/PKGBUILD

# build
cd build
makepkg -rsf
```

Your package will be the file with the `.pkg.tar.zst` extension in `build/`

### Alpine

Alpine needs a bit of setup on the first time, but the [documentation](https://wiki.alpinelinux.org/wiki/Creating_an_Alpine_package) is excellent.

``` bash
# install some packages, create a user, give it power and a key
apk add -q --no-progress --update tar alpine-sdk sudo
adduser -D builder
echo "builder ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers
addgroup builder abuild
su builder -c "abuild-keygen -nai"
```

Then, to actually build your package:

``` bash
# create a work directory
mkdir build
# copy the tarball and PKGBUIL file, substituing the variables we care about
cp vmod_reqwest-$VMOD_VERSION.tar.gz build
sed -e "s/@VMOD_VERSION@/$VMOD_VERSION/" -e "s/@VARNISH_VERSION@/$VARNISH_VERSION/" pkg/arch/APKBUILD > build/APKBUILD

su builder -c "abuild checksum"
su builder -c "abuild -r"
```
