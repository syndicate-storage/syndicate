%define PKG_NAME libmicrohttpd-syndicate
%define SRC_TAR %{PKG_NAME}.tar.gz
%define VERSION 0.01
%define RELEASE 1

Summary: Syndicate-specific libmicrohttpd implementation
Name: %{PKG_NAME}
Version: %{VERSION}
Release: %{RELEASE}
License: GPL
Group: Development/Tools
Requires: gcc gcc-c++ gnutls-devel openssl-devel boost-devel libxml2-devel libattr-devel curl-devel automake make libtool texinfo libgcrypt-devel
BuildRequires: gcc gcc-c++ gnutls-devel openssl-devel boost-devel libxml2-devel libattr-devel curl-devel automake make libtool texinfo libgcrypt-devel
Provides: libmicrohttpd
BuildArch: i386
Source: %{SRC_TAR}
BuildRoot: %{_tmppath}/%{PKG_NAME}-%{VERSION}-%{RELEASE}

%define instdir /

%description
Support libraries and header files for libmicrohttpd, specific to Syndicate.

%prep
%setup -n %{PKG_NAME}

%build
./bootstrap
./configure --prefix=/usr
make

%install
make DESTDIR=$RPM_BUILD_ROOT/ install 

%post
/sbin/ldconfig

%clean
make clean

%files
%defattr(-,root,root)
/usr/lib/libmicrohttpd.*
/usr/include/microhttpd.*
/usr/lib/pkgconfig/libmicrohttpd.*
/usr/share/info/microhttpd*
/usr/share/man/man3/libmicrohttpd.*
