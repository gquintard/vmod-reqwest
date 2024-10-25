ARG RUST_VERSION
ARG VARNISH_VERSION

FROM rust:${RUST_VERSION}

WORKDIR /vmod_reqwest
ARG VMOD_REQWEST_VERSION
ARG RELEASE_URL=https://github.com/gquintard/vmod_reqwest/archive/refs/tags/v${VMOD_REQWEST_VERSION}.tar.gz

ARG VARNISH_VERSION_NODOT
ENV VARNISH_VERSION_NODOT=$VARNISH_VERSION_NODOT
RUN env && curl -s https://packagecloud.io/install/repositories/varnishcache/varnish${VARNISH_VERSION_NODOT}/script.deb.sh | bash && apt-get update && apt-get install -y varnish-dev clang libssl-dev

RUN curl -Lo dist.tar.gz ${RELEASE_URL} && \
    tar xavf dist.tar.gz --strip-components=1 && \
    cargo build --release

FROM varnish:${VARNISH_VERSION}
COPY --from=0 /vmod_reqwest/target/release/libvmod_reqwest.so /usr/lib/varnish/vmods/
