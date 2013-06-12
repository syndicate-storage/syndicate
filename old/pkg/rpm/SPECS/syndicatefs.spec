%define PKG_NAME syndicatefs
%define SRC_TAR %{PKG_NAME}.tar.gz
%define VERSION 0.01
%define RELEASE 1

Summary: Syndicate filesystem client
Name: %{PKG_NAME}
Version: %{VERSION}
Release: %{RELEASE}
License: Apache
Group: Development/Tools
Requires: fuse curl libxml2 gnutls openssl boost libattr fcgi libgcrypt libmicrohttpd-syndicate libsyndicate
BuildRequires: fuse-devel curl-devel libxml2-devel gnutls-devel openssl-devel boost-devel libattr-devel fcgi-devel libgcrypt-devel libmicrohttpd-syndicate libsyndicate automake make libtool texinfo gcc gcc-c++
Provides: syndicatefs
BuildArch: i386
Source: %{SRC_TAR}
BuildRoot: %{_tmppath}/%{PKG_NAME}-%{VERSION}-%{RELEASE}

%define instdir /

%description
Syndicate filesystem client

%prep
%setup -n %{PKG_NAME}

%build
make

%install
make DESTDIR=$RPM_BUILD_ROOT/ install

%post
/sbin/ldconfig

%postun
make uninstall

%clean
make clean

%files
%defattr(-,root,root)
%dir /etc/syndicate
/usr/bin/syndicatefs*
/etc/syndicate/syndicate-client.conf
