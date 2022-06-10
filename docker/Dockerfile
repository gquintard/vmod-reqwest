FROM rust:1.61-buster

WORKDIR /vmod_reqwest

ARG vmod_reqwest_version=0.0.3
ARG github_release_url=/refs/tags/v${vmod_reqwest_version}.zip

RUN curl -s https://packagecloud.io/install/repositories/varnishcache/varnish71/script.deb.sh | bash && apt-get update && apt-get install -y varnish-dev clang libssl-dev

RUN wget https://github.com/gquintard/vmod_reqwest/archive${github_release_url} && unzip v${vmod_reqwest_version}.zip && cd vmod_reqwest-${vmod_reqwest_version} && cargo build --release
