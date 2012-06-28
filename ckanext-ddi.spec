%define name ckanext-ddi
%define version 0.1
%define unmangled_version 0.1
%define unmangled_version 0.1
%define release 1

Summary: DDI Importing tools for CKAN
Name: %{name}
Version: %{version}
Release: %{release}
Source0: %{name}-%{unmangled_version}.tar.gz
License: AGPL
Group: Development/Libraries
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}-buildroot
Prefix: %{_prefix}
BuildArch: x86_64
Vendor: Aleksi Suomalainen <aleksi.suomalainen@nomovok.com>
Url: http://not.there.yet
Requires: libxslt
Requires: kata-ckan-prod
BuildRequires: kata-ckan-dev

%description
DDI importing tools for KATA ckan.

%prep
%setup -n %{name}-%{unmangled_version} -n %{name}-%{unmangled_version}

%build
source /home/ckan/pyenv/bin/activate
python setup.py build

%install
source /home/ckan/pyenv/bin/activate
python setup.py install --single-version-externally-managed -O1 --root=$RPM_BUILD_ROOT --record=INSTALLED_FILES
pip install --no-deps lxml==2.3.4 --install-option="--root=$RPM_BUILD_ROOT" --install-option="--record=INST"
%post
#source /home/ckan/pyenv/bin/activate
#pip install  lxml 
%clean
rm -rf $RPM_BUILD_ROOT

%files -f INSTALLED_FILES 
/home/ckan/pyenv/lib64/python2.6/site-packages/lxml
/home/ckan/pyenv/lib64/python2.6/site-packages/lxml-2.3.4-py2.6.egg-info
%defattr(-,root,root)
