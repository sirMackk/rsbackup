# RSBackup

RSBackup is a project I made for **fun** and for practicing some techniques, like Python's typehints. Its a client-server program that works to back up data more reliably thanks to [Reed-Solomon error correction][0].

The current version is the first prototype. The client talks http with the server to upload/check/repair/retrieve files. The server manages the files. It saves a local copy of the submitted data along with additional files, which are parity shards. If a part of the original file becomes corrupted, the server can detect this and repair the damage unless the corruption is too big. A simple way to figure this out is to consider how many shards of the original data file are corrupt–say you've split the data file into 10 data shards and created 3 parity shards. You can lose up to 3/13ths or around 23% of the data, including the parity shards, and still rebuild the original file. Pretty cool.

The parity shards are kept on the same filesystem as the data file. This only weakly enchances data durability. It's possible to improve this scheme by storing the shards (data & parity) on different devices or servers. If each of the 13 shards was moved to its own server, especially in a different failure domain (think: server, rack, datacenter hall, datacenter, continent, etc.) then the chance that 3 of those servers fail is very small, meaning that our data is very safe.

[0]: https://en.wikipedia.org/wiki/Reed%E2%80%93Solomon_error_correction

# LICENSE

Copyright 2020 sirmackk

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
