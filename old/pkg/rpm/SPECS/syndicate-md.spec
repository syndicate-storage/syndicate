%define PKG_NAME syndicate-md
%define SRC_TAR %{PKG_NAME}.tar.gz
%define VERSION 0.01
%define RELEASE 1

Summary: Metadata server for the Syndicate filesystem
Name: %{PKG_NAME}
Version: %{VERSION}
Release: %{RELEASE}
License: Apache
Group: Development/Tools
Requires: curl libsyndicate libmicrohttpd-syndicate openssl libgcrypt gnutls openssl
BuildRequires: curl-devel libsyndicate libmicrohttpd-syndicate openssl-devel libgcrypt-devel gnutls-devel openssl-devel
Provides: mdserverd
BuildArch: i386
Source: %{SRC_TAR}
BuildRoot: %{_tmppath}/%{PKG_NAME}-%{VERSION}-%{RELEASE}

%define instdir /

%description
Metadata server daemon for the Syndicate filesystem

%prep
%setup -n %{PKG_NAME}

%build
make

%install
make DESTDIR=$RPM_BUILD_ROOT/ install

%post
/sbin/ldconfig

%preun
/etc/init.d/syndicate-md stop
stopped=$?
if ! [[ stopped ]]; then
   /etc/init.d/syndicate-md force-stop
fi

%clean
make clean

%files
%defattr(-,root,root)
%dir /etc/syndicate
/usr/bin/mdserverd
/usr/bin/mdtool
/etc/syndicate/syndicate-metadata-server.conf
/etc/init.d/syndicate-md
