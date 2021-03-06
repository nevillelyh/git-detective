#!/usr/bin/env python

import copy
import difflib
import hashlib
import os
import re
import sys

import git
import nltk

snapshot = {}

file_action = set(('create', 'remove', 'modify', 'insert'))
line_action = set(('insert', 'delete', 'change'))

action_schema = {
        'commit': 0,
        'no_msg': 0,
        'insert': 0, 'delete': 0, 'change': 0,
        'create': 0, 'remove': 0, 'rename': 0, 'modify': 0,
        }

author_schema = {
        'global': copy.deepcopy(action_schema),
        'path': {},
        }

path_schema = {
        'global': copy.deepcopy(action_schema),
        'author': {},
        }

message_schema = {
        'term': {},
        'bigram': {},
        'trigram': {},
        }

conflict_schema = {
        'delete': 0, 'change': 0, 'total': 0,
        'self_delete': 0, 'self_change': 0, 'self_total': 0,
        'peer_delete': 0, 'peer_change': 0, 'peer_total': 0,
        }

ignore_pattern = ()

global_stat = copy.deepcopy(action_schema)
author_stat = {}
path_stat = {}
conflict_stat = {}
message_stat = {
        'global': copy.deepcopy(message_schema),
        'author': {},
        }

def hash(string):
    return hashlib.sha1(string).hexdigest()

def resolve_diffset(diffset):
    new_dict = {}
    del_dict = {}
    ren_list = []
    mod_list = []
    for diff in diffset:
        if diff.new_file:
            key = hash(diff.b_blob.data_stream.read())
            new_dict.setdefault(key, []).append(diff)
        elif diff.deleted_file:
            key = hash(diff.a_blob.data_stream.read())
            del_dict.setdefault(key, []).append(diff)
        elif diff.renamed:
            ren_list.append(diff)
        else:
            mod_list.append(diff)

    new_list = []
    del_list = []

    new_keys = set(new_dict.keys())
    del_keys = set(del_dict.keys())
    for key in new_keys.intersection(del_keys):
        if len(new_dict[key]) == 1 and len(del_dict[key]) == 1:
            ren_list.append((del_dict[key][0], new_dict[key][0]))
            new_dict.pop(key)
            del_dict.pop(key)

    for val in new_dict.values():
        new_list += val
    for val in del_dict.values():
        del_list += val

    return {
            'new': new_list,
            'del': del_list,
            'ren': ren_list,
            'mod': mod_list,
            }

def cleanup_message(message):
    line = message.splitlines()
    content = []
    for l in line:
        if l.strip().startswith('git-svn-id'):
            continue
        content.append(l)
    return '\n'.join(content)

def index_message(author, message):
    tokenizer = nltk.tokenize.WordPunctTokenizer()
    message_stat['author'].setdefault(author, copy.deepcopy(message_schema))
    for l in message.splitlines():
        for p in ignore_pattern:
            l = re.sub(p, '', l)
        tokens = []
        for t in tokenizer.tokenize(l):
            if len(t) > 1:
                tokens.append(t)
        for t in tokens:
            message_stat['global']['term'][t] = message_stat['global']['term'].get(t, 0) + 1
            message_stat['author'][author]['term'][t] = message_stat['author'][author]['term'].get(t, 0) + 1
        for t in nltk.util.bigrams(tokens):
            message_stat['global']['bigram'][t] = message_stat['global']['bigram'].get(t, 0) + 1
            message_stat['author'][author]['bigram'][t] = message_stat['author'][author]['bigram'].get(t, 0) + 1
        for t in nltk.util.trigrams(tokens):
            message_stat['global']['trigram'][t] = message_stat['global']['trigram'].get(t, 0) + 1
            message_stat['author'][author]['trigram'][t] = message_stat['author'][author]['trigram'].get(t, 0) + 1

def replay_action(action, author, path=None, last_author=None, last_path=None, message=None):
    global_stat[action] += 1

    author_stat.setdefault(author, copy.deepcopy(author_schema))
    if path:
        if last_path:
            for a in author_stat:
                if last_path in author_stat[a]['path']:
                    author_stat[a]['path'][path] = author_stat[a]['path'].pop(last_path)
            path_stat[path] = path_stat.pop(last_path)
        author_stat[author]['path'].setdefault(path, copy.deepcopy(action_schema))
        path_stat.setdefault(path, copy.deepcopy(path_schema))
        path_stat[path]['author'].setdefault(author, copy.deepcopy(action_schema))

    if action == 'commit':
        author_stat[author]['global']['commit'] += 1
        message = cleanup_message(message)
        if message.strip() == '':
            author_stat[author]['global']['no_msg'] += 1
            global_stat['no_msg'] += 1
        index_message(author, message)
    elif action in file_action or action in line_action:
        author_stat[author]['global'][action] += 1
        author_stat[author]['path'][path][action] += 1

        path_stat[path]['global'][action] += 1
        path_stat[path]['author'][author][action] += 1

    if last_author:
        conflict_stat.setdefault(author, {}).setdefault(last_author, copy.deepcopy(action_schema))[action] += 1

def replay_new(author, path, content):
    # print 'NEW', author, path
    assert path not in snapshot
    c = content.splitlines()
    snapshot[path] = []
    for l in c:
        snapshot[path].append((author, l))
        replay_action('insert', author, path)
    replay_action('create', author, path)

def replay_del(author, path, content):
    # print 'DEL', author, path
    c = content.splitlines()
    assert [l[1] for l in snapshot[path]] == c
    for l in snapshot[path]:
        replay_action('delete', author, path, last_author=l[0])
    snapshot.pop(path)
    replay_action('remove', author, path)

def replay_ren(author, oldpath, newpath):
    # print 'REN', author, oldpath, newpath
    assert newpath not in snapshot
    snapshot[newpath] = snapshot.pop(oldpath)
    replay_action('rename', author, newpath, last_path=oldpath)

def replay_mod(author, path, a_content, b_content):
    # print 'MOD', author, path
    a = a_content.splitlines()
    b = b_content.splitlines()
    new_snapshot = []
    for tag, i1, i2, j1, j2 in difflib.SequenceMatcher(None, a, b).get_opcodes():
        if tag == 'equal':
            for i in range(i1, i2):
                new_snapshot.append(snapshot[path][i])
        elif tag == 'insert':
            for j in range(j1, j2):
                new_snapshot.append((author, b[j]))
                replay_action(tag, author, path)
        elif tag == 'replace':
            for i in range(i1, i2):
                replay_action('change', author, path, last_author=snapshot[path][i][0])
            for j in range(j1, j2):
                new_snapshot.append((author, b[j]))
        elif tag == 'delete':
            for i in range(i1, i2):
                replay_action('delete', author, path, last_author=snapshot[path][i][0])
    replay_action('modify', author, path)

    snapshot[path] = new_snapshot
    assert [l[1] for l in snapshot[path]] == b

def replay_commit(commit, diffset):
    author = unicode(commit.author)
    print >> sys.stderr, 'commit %s' % unicode(commit)

    diff = resolve_diffset(diffset)

    for d in diff['new']:
        replay_new(author, d.b_blob.path, d.b_blob.data_stream.read())
    for d in diff['del']:
        replay_del(author, d.a_blob.path, d.a_blob.data_stream.read())
    for d in diff['ren']:
        if type(d) is tuple:
            replay_ren(author, d[0].a_blob.path, d[1].b_blob.path)
        else:
            replay_ren(author, d.rename_from, d.rename_to)
    for d in diff['mod']:
        assert d.a_blob.path == d.b_blob.path
        replay_mod(author, d.b_blob.path, d.a_blob.data_stream.read(), d.b_blob.data_stream.read())
    replay_action('commit', author, message=commit.message)

def replay_log(repo):
    prev = None
    for curr in repo.iter_commits(reverse=True):
        if prev:
            diffset = prev.diff(curr)
            replay_commit(curr, diffset)
        else:
            for stage, blob in git.index.base.IndexFile.from_tree(repo, curr.tree).iter_blobs():
                replay_new(unicode(curr.author), blob.path, blob.data_stream.read())
        prev = curr

def stat_summary(stat):
    return '\t'.join(['%s\t%d' % (key, val) for key, val in stat.items()])

def report():
    print '#' * 80
    print 'Global:\t%s' % stat_summary(global_stat)
    print

    print '#' * 80
    for author, stat in author_stat.items():
        print 'Author:\t%10s\t%s' % (author, stat_summary(stat['global']))
        print 'Author-Path:\t%d' % len(stat['path'])
        for path in stat['path']:
            print '\t%s' % path
            print '\t%s' % stat_summary(stat['path'][path])
        print

    print '#' * 80
    for path, stat in path_stat.items():
        print 'Path:\t%s\t%s' % (path, stat_summary(stat['global']))
        print 'Path-Author:\t%d' % len(stat['author'])
        for author in stat['author']:
            print '\t%s' % author
            print '\t%s' % stat_summary(stat['author'][author])
        print

    conflict_list = []
    conflict_recv = {}
    conflict_made = {}
    for editor, stat in conflict_stat.items():
        for author in stat:
            total = stat[author]['delete'] + stat[author]['change']
            conflict_list.append((editor, author, stat[author]['delete'], stat[author]['change'], total))

            conflict_made.setdefault(editor, copy.deepcopy(conflict_schema))
            conflict_made[editor]['delete'] += stat[author]['delete']
            conflict_made[editor]['change'] += stat[author]['change']
            conflict_made[editor]['total'] += total
            if editor == author:
                conflict_made[editor]['self_delete'] += stat[author]['delete']
                conflict_made[editor]['self_change'] += stat[author]['change']
                conflict_made[editor]['self_total'] += total
            else:
                conflict_made[editor]['peer_delete'] += stat[author]['delete']
                conflict_made[editor]['peer_change'] += stat[author]['change']
                conflict_made[editor]['peer_total'] += total

            conflict_recv.setdefault(author, copy.deepcopy(conflict_schema))
            conflict_recv[author]['delete'] += stat[author]['delete']
            conflict_recv[author]['change'] += stat[author]['change']
            conflict_recv[author]['total'] += total
            if editor == author:
                conflict_recv[author]['self_delete'] += stat[author]['delete']
                conflict_recv[author]['self_change'] += stat[author]['change']
                conflict_recv[author]['self_total'] += total
            else:
                conflict_recv[author]['peer_delete'] += stat[author]['delete']
                conflict_recv[author]['peer_change'] += stat[author]['change']
                conflict_recv[author]['peer_total'] += total

    conflict_list.sort(reverse=True, key=lambda x: x[4])
    print '#' * 80
    print '# Conflits'
    print '# %10s\t%10s\tdelete\tchange\ttotal' % ('editor', 'author')
    for c in conflict_list:
        print '%10s\t%10s\t%d\t%d\t%d' % c
    print

    print '#' * 80
    print '# Conflits made'
    print '# %10s\tdelete\tchange\ttotal\ts_del\ts_chg\ts_total\tp_del\tp_chg\tp_total' % 'editor'
    for c in sorted(conflict_made.items(), reverse=True, key=lambda x: x[1]['total']):
        print '%10s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d' % (
                c[0], c[1]['delete'], c[1]['change'], c[1]['total'],
                c[1]['self_delete'], c[1]['self_change'], c[1]['self_total'],
                c[1]['peer_delete'], c[1]['peer_change'], c[1]['peer_total'],
                )
    print

    print '#' * 80
    print '# Conflits received'
    print '# %10s\tdelete\tchange\ttotal\ts_del\ts_chg\ts_total\tp_del\tp_chg\tp_total' % 'author'
    for c in sorted(conflict_recv.items(), reverse=True, key=lambda x: x[1]['total']):
        print '%10s\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d' % (
                c[0], c[1]['delete'], c[1]['change'], c[1]['total'],
                c[1]['self_delete'], c[1]['self_change'], c[1]['self_total'],
                c[1]['peer_delete'], c[1]['peer_change'], c[1]['peer_total'],
                )
    print

    print '#' * 80
    print '# Commit message'
    print '# Term'
    print ' '.join(['%s[%d]' % i for i in sorted(message_stat['global']['term'].items(), reverse=True, key=lambda x:x[1])])
    print '# Bigram'
    print ' '.join(['"%s"[%d]' % (' '.join(i[0]), i[1]) for i in sorted(message_stat['global']['bigram'].items(), reverse=True, key=lambda x:x[1])])
    print '# Trigram'
    print ' '.join(['"%s"[%d]' % (' '.join(i[0]), i[1]) for i in sorted(message_stat['global']['trigram'].items(), reverse=True, key=lambda x:x[1])])

    for author in message_stat['author']:
        print '#' * 80
        print '# Commit message by %s' % author
        print '# Term'
        print ' '.join(['%s[%d]' % i for i in sorted(message_stat['author'][author]['term'].items(), reverse=True, key=lambda x:x[1])])
        print '# Bigram'
        print ' '.join(['"%s"[%d]' % (' '.join(i[0]), i[1]) for i in sorted(message_stat['author'][author]['bigram'].items(), reverse=True, key=lambda x:x[1])])
        print '# Trigram'
        print ' '.join(['"%s"[%d]' % (' '.join(i[0]), i[1]) for i in sorted(message_stat['author'][author]['trigram'].items(), reverse=True, key=lambda x:x[1])])

def main():
    if len(sys.argv) != 2:
        print 'Usage: %s [REPO]' % os.path.basename(sys.argv[0])
        sys.exit(1)

    repopath = sys.argv[1]
    repo = git.Repo(repopath)

    replay_log(repo)
    report()

if __name__ == '__main__':
    main()
