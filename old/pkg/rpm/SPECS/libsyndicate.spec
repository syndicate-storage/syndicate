%define PKG_NAME libsyndicate
%define SRC_TAR %{PKG_NAME}.tar.gz
%define VERSION 0.01
%define RELEASE 1

Summary: Support libraries for the Syndicate filesystem
Name: %{PKG_NAME}
Version: %{VERSION}
Release: %{RELEASE}
License: Apache
Group: Development/Tools
Requires: gcc gcc-c++ gnutls-devel openssl-devel libattr-devel curl-devel automake make libtool texinfo libgcrypt-devel libmicrohttpd-syndicate
BuildRequires: gcc gcc-c++ gnutls-devel openssl-devel libattr-devel curl-devel automake make libtool texinfo libgcrypt-devel libmicrohttpd-syndicate
Provides: libsyndicate.so
BuildArch: i386
Source: %{SRC_TAR}
BuildRoot: %{_tmppath}/%{PKG_NAME}-%{VERSION}-%{RELEASE}

%define instdir /

%description
Support libraries and header files for the Syndicate filesystem

%prep
%setup -n %{PKG_NAME}

%build
make

%install
make DESTDIR=$RPM_BUILD_ROOT/ install 

%post
/sbin/ldconfig

%clean
make clean

%files
%defattr(-,root,root)
%dir /usr/include/syndicate
/usr/lib/libsyndicate.*
/usr/include/syndicate/*.h
