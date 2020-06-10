#!/usr/bin/perl
use warnings;
my $other_branch = "upstream/branch-3.0";
#my $last_commit = "7ab167a9952c363306f0b9ee7402482072039d2b";
my $last_commit = "c0632cec04e5b0f3fb3c3f27c21a2d3f3fbb4f7e";
my $other_git_log = `git log --format=oneline $other_branch...$last_commit`;
my $long_git_log = `git log --decorate --color=always -p $other_branch...$last_commit`;
my $current_log = `git log --format=oneline`;
my @commits_code = reverse(split('\n\S*commit\s+', $long_git_log));
my $commits_code_size = $#commits_code;
my @commits_desc = reverse(split('\n', $other_git_log));
my $commits_desc_size = $#commits_desc;
my $new_changes = "";
# Load existing changes
open my $fh, '<', 'APPLE_CHANGES.txt' or die "Can't open file $!";
my $apple_changes = do { local $/; <$fh> };

# Process the commits
if ($commits_desc_size != $commits_code_size) {
    print "Error $commits_desc_size != $commits_code_size";
    exit;
}
COMMIT: foreach my $i (0..$#commits_desc) {
    my $commit_desc = $commits_desc[$i];
    my $commit_code = $commits_code[$i];
    my $commit;
    my $jira;
    my $desc;
    # Extract the info we need for the log for commits of [SPARK-...]
    if ($commit_desc =~ /([a-f0-9]{40})\s+\[(SPARK-\d+)\](.*\])\s*(.+?)$/) {
	$commit = $1;
	$jira = $2;
	$desc = $4;
	# Sometimes people don't mention the component
    } elsif ($commit_desc =~ /([a-f0-9]{40})\s+\[(SPARK-\d+)\]\s*(.+?)$/) {
	$commit = $1;
	$jira = $2;
	$desc = $3;
	# Sometimes we have hotfixes and the like
    } elsif ($commit_desc =~ /([a-f0-9]{40})\s+(.+)/) {
	$commit = $1;
	$desc = $2;
	$jira = "UNSET";
    } else {
	print "Can't handle $commit_desc please merge manually";
    }
    my $jira_merged = "";
    my $default = "";
    if ($current_log =~ /$jira/) {
	if ($jira ne "UNSET") {
	    $jira_merged = "JIRA ALREAD MERGED";
	}
	if ($commit_desc !~ /HOTFIX/i || $commit_desc !~ /MINOR/i || $commit_desc !~ /FOLLOW\s*UP/i) {
	    $default = "n";
	    my $quoted_desc= quotemeta(chomp($commit_desc));
	    if ($current_log =~ /$quoted_desc/i) {
		next COMMIT;
	    }
	}
    }
    if ($apple_changes =~ /$jira/ && $jira_merged eq "") {
	$jira_merged = "*****Present in APPLE_CHANGES.txt but not in git log?****";
    }
    print "Code:\n$commit_code\n";
    print "Merge commit $commit_desc with $jira? $jira_merged [y/n]";
    while (my $input = <>) {
	chomp($input);
	if ($input eq 'y') {
	    $new_changes = "$jira\t $desc\n$new_changes";
	    my $merge_result = system("git cherry-pick $commit");
	    print "Result:\n$merge_result";
	    if ($merge_result != 0) {
		print "Git merge issue. Press enter when fixed";
		my $i = <>;
	    }
	    last;
	} elsif ($input eq 'n' || ($input eq '' && $default eq 'n')) {
	    last;
	} else {
	    print "Invalid answer $input\n";
	}
    }
}
print "Please update APPLE_CHANGES with:\n$new_changes\n";
