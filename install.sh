#!/bin/sh
# Installs the latest release of temporal-operator-mcp into ~/.temporalio/bin:
#
#   curl -sSfL https://raw.githubusercontent.com/temporalio/temporal-operator-mcp/main/install.sh | sh
set -eu

repo=https://github.com/temporalio/temporal-operator-mcp

fail() {
	echo "install.sh: $*" >&2
	exit 1
}

fetch() {
	curl --proto '=https' --tlsv1.2 -fsSL "$@"
}

# Everything runs from main so that a partial download of this script runs nothing.
main() {
	case "$(uname -s)" in
	Darwin) os=darwin ;;
	Linux) os=linux ;;
	*) fail "unsupported OS $(uname -s): releases are built for macOS and Linux" ;;
	esac
	case "$(uname -m)" in
	x86_64 | amd64) arch=amd64 ;;
	arm64 | aarch64) arch=arm64 ;;
	*) fail "unsupported architecture $(uname -m): releases are built for amd64 and arm64" ;;
	esac

	# /releases/latest redirects to the latest release's page, which ends in its tag.
	# Unlike the GitHub API, it isn't rate limited.
	url=$(fetch -I -o /dev/null -w '%{url_effective}' "$repo/releases/latest")
	tag=${url##*/releases/tag/}
	[ "$tag" != "$url" ] || fail "found no release at $repo/releases"
	archive="temporal-operator-mcp_${tag#v}_${os}_${arch}.tar.gz"

	tmp=$(mktemp -d)
	trap 'rm -rf "$tmp"' EXIT
	trap 'exit 1' HUP INT TERM

	echo "Downloading $archive from $tag"
	fetch -o "$tmp/$archive" "$repo/releases/download/$tag/$archive"
	fetch -o "$tmp/checksums.txt" "$repo/releases/download/$tag/checksums.txt"

	expected=$(awk -v f="$archive" '$2 == f { print $1 }' "$tmp/checksums.txt")
	[ -n "$expected" ] || fail "checksums.txt has no entry for $archive"
	if command -v sha256sum >/dev/null; then
		actual=$(sha256sum "$tmp/$archive")
	else
		actual=$(shasum -a 256 "$tmp/$archive")
	fi
	actual=${actual%% *}
	[ "$actual" = "$expected" ] || fail "$archive has SHA-256 $actual, but checksums.txt says $expected"

	bin="$HOME/.temporalio/bin"
	# -o gives the binary to the user running this, not to the release's builder.
	tar -xzof "$tmp/$archive" -C "$tmp" temporal-operator-mcp
	mkdir -p "$bin"
	mv "$tmp/temporal-operator-mcp" "$bin/"
	echo "Installed $bin/temporal-operator-mcp"

	case ":$PATH:" in
	*":$bin:"*) ;;
	*)
		echo "$bin isn't on your PATH. To add it, add this line to your shell's startup file, such as ~/.zshrc or ~/.bashrc, and open a new terminal:"
		# shellcheck disable=SC2016 # $HOME and $PATH are for the startup file to expand.
		echo '  export PATH="$HOME/.temporalio/bin:$PATH"'
		;;
	esac
}

main
