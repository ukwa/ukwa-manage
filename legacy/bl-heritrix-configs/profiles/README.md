# Profiles

The directory contains all Heritrix profiles currently (or recently) in use.

#### includes.xml

This file contains elements common to all profiles. Within each individual profile, elements are included via the [xinclude](http://www.w3.org/TR/xinclude/) model. Specifically, they will contain lines like:

    <xi:include href="includes.xml" xpointer="xpointer(//includes/excludes/list)" />

Specifically it includes:

1. A list of regular expressions to be excluded from all crawls.
2. The [Sheets](https://webarchive.jira.com/wiki/display/Heritrix/Sheets) currently in use.

#### profile-frequent.cxml
This is profile used for all frequency-based (`daily`, `weekly`, etc.) crawls.

#### profile-domain.cxml
This was the profile used in the 2012, 2013 and the start of the 2014 domain crawls. Following the _Cookie Monster_ bug and the subsequent move to Heritrix 3.3.0-SNAPSHOT (and the myriad of undocumented changes that brought) this profile became deprecated.

#### profile-domain-3.3.0.cxml
This was the profile used in the latter part of the 2014 domain crawl (and to be used in the 2015 domain crawl).

#### profile-slash-page.cxml
This is the profile used for slash-page crawls. Unlike the other profiles, this does not use the regular expressions from `includes.xml` (due to the more limited `DecideRule` sequence) and only uses _some_ of the Sheets.

