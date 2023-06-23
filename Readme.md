# My Tiny Key/Value Storage Engine

This is a KV storage engine that is useless to you but useful and meaningful to me.

The whole structure is based on [Bitcask](https://riak.com/assets/bitcask-intro.pdf). Thus, I update data by appending logs rather than update-in-place.

I have implemented all the following functions from scratch:

- Basic operations: Put, Delete, Get, ListKeys, etc.
- DataBase Baisc operation: Open, Close, Sync, Iterator, Merge, Backup.
- Support transaction by implementing WriteBatch.
- Optimize memory index (support ART, B+ Tree, B Tree), optimize file IO using MMap to speed up file reading, provide database state query to speed merge process.
- Support some interface: HTTP, Redis(String, Hash, Set, List, Sorted Set).
- All operations are accompanied by comprehensive testing scripts and have undergone overall database benchmark testing.



Although the project is somewhat difficult, but overall, it is still a toy:)

Stay Hungry Stay Foolish !