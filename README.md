# Rust sqlite

## variable-length integer(varint)
A variable-length integer or "varint" is a static Huffman encoding of 64-bit twos-complement integers that uses less space for small positive values. A varint is between 1 and 9 bytes in length. The varint consists of either zero or more bytes which have the high-order bit set followed by a single byte with the high-order bit clear, or nine bytes, whichever is shorter. The lower seven bits of each of the first eight bytes and all 8 bits of the ninth byte are used to reconstruct the 64-bit twos-complement integer. Varints are big-endian: bits taken from the earlier byte of the varint are more significant than bits taken from the later bytes. 

first page

len: 
record.columns[0] ~ [5]

5
String("table")
String("apples")
String("apples")
I8(2)
String("CREATE TABLE apples\n(\n\tid integer primary key autoincrement,\n\tname text,\n\tcolor text\n)")
5
String("table")
String("sqlite_sequence")
String("sqlite_sequence")
I8(3)
String("CREATE TABLE sqlite_sequence(name,seq)")
5
String("table")
String("oranges")
String("oranges")
I8(4)
String("CREATE TABLE oranges\n(\n\tid integer primary key autoincrement,\n\tname text,\n\tdescription text\n)")


## Resources
- [Database File Format](https://www.sqlite.org/fileformat.html)
- [books](https://www.sqlite.org/books.html)
- [Busying Oneself With B-Trees](https://medium.com/basecs/busying-oneself-with-b-trees-78bbf10522e7)
- [SQLIte database file format diagrams](https://saveriomiroddi.github.io/SQLIte-database-file-format-diagrams/)
