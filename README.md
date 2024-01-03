# Rust sqlite

## Command

### .dbinfo

```bash

./your_sqlite3.sh .dbinfo sample.db

# Output

# database page size: 4096
# number of tables: 2

```

### .tables

```bash

./your_sqlite3.sh .tables sample.db

# Output

# apples   oranges

```

### .query

1. Read the count of rows from table


```bash

./your_sqlite3.sh .query sample.db "SELECT COUNT(*) FROM apples""

# Output

# 4
```

2. Read data from rows


```bash

./your_sqlite3.sh .query sample.db "SELECT name FROM apples"

# Output

# Granny Smith
# Fuji
# Honeycrisp
# Golden Delicious
```

3. Query for multiple columns

```bash

./your_sqlite3.sh .query sample.db "SELECT name, color FROM apples"

# Output

# Granny Smith|Light Green
# Fuji|Red
# Honeycrisp|Blush Red
# Golden Delicious|Yellow

```

4. Using a `WHERE` clause


```bash

./your_sqlite3.sh sample.db "SELECT name, color FROM apples WHERE color = 'Yellow'"

# Output

# Golden Delicious|Yellow

```

5. Using a full-table scan


```bash

./your_sqlite3.sh sample.db "SELECT name, color FROM apples WHERE color = 'Yellow'"

# Output

# 297|Stealth (New Earth)
# 790|Tobias Whale (New Earth)
# 1085|Felicity (New Earth)
# 2729|Thrust (New Earth)
# 3289|Angora Lapin (New Earth)
# 3913|Matris Ater Clementia (New Earth)

```

6. Using an index


```bash

./your_sqlite3.sh companies.db "SELECT id, name FROM companies WHERE country = 'eritrea'"

# Output

# 121311|unilink s.c.
# 2102438|orange asmara it solutions
# 5729848|zara mining share company
# 6634629|asmara rental

```

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
- [SQLite Internals: Pages & B-trees](https://fly.io/blog/sqlite-internals-btree/)
