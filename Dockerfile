FROM rust:1.67-buster

WORKDIR /vmod_reqwest
ARG VMOD_REQWEST_VERSION=0.0.8
ARG RELEASE_URL=https://github.com/gquintard/vmod_reqwest/archive/refs/tags/v${VMOD_REQWEST_VERSION}.tar.gz

RUN curl -s https://packagecloud.io/install/repositories/varnishcache/varnish73/script.deb.sh | bash && apt-get update && apt-get install -y varnish-dev clang libssl-dev

RUN curl -Lo dist.tar.gz ${RELEASE_URL} && \
    tar xavf dist.tar.gz --strip-components=1 && \
    cargo build --release

FROM varnish:7.3
COPY --from=0 /vmod_reqwest/target/release/libvmod_reqwest.so /usr/lib/varnish/vmods/
