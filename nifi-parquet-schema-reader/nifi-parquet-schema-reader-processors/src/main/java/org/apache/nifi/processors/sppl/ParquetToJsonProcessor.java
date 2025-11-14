package org.apache.nifi.processors.sppl;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.ProcessorInitializationContext;

import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;

import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;

import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import java.util.HashSet;
import java.util.Set;

@SupportsBatching
@Tags({"parquet", "json", "convert", "nifi", "streaming"})
@CapabilityDescription("Converts Parquet binary data from a FlowFile into JSON using the low-level Parquet reader. No Hadoop or Avro dependencies are used.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "application/json")
})
public class ParquetToJsonProcessor extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Successfully converted Parquet to JSON")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failed to convert")
            .build();

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    /* ============================================================
       Seekable InputStream backed by a byte[]
       ============================================================ */
    public static class ByteArraySeekableInputStream extends SeekableInputStream {
        private final byte[] data;
        private int pos = 0;
        private int mark = -1;

        public ByteArraySeekableInputStream(byte[] data) {
            this.data = data;
        }

        @Override
        public int read() {
            if (pos >= data.length) return -1;
            return data[pos++] & 0xFF;
        }

        @Override
        public int read(byte[] b, int off, int len) {
            if (pos >= data.length) return -1;
            int toRead = Math.min(len, data.length - pos);
            System.arraycopy(data, pos, b, off, toRead);
            pos += toRead;
            return toRead;
        }

        @Override
        public int read(ByteBuffer dst) {
            int remaining = data.length - pos;
            if (remaining <= 0) return -1;

            int toRead = Math.min(dst.remaining(), remaining);
            dst.put(data, pos, toRead);
            pos += toRead;
            return toRead;
        }

        @Override
        public void readFully(byte[] b) {
            readFully(b, 0, b.length);
        }

        @Override
        public void readFully(byte[] b, int off, int len) {
            if (pos + len > data.length) {
                throw new RuntimeException(new EOFException("Unexpected EOF"));
            }
            System.arraycopy(data, pos, b, off, len);
            pos += len;
        }

        @Override
        public void readFully(ByteBuffer buf) {
            int len = buf.remaining();
            if (pos + len > data.length) {
                throw new RuntimeException(new EOFException("Unexpected EOF"));
            }
            buf.put(data, pos, len);
            pos += len;
        }

        @Override
        public long getPos() {
            return pos;
        }

        @Override
        public void seek(long newPos) {
            if (newPos < 0 || newPos > data.length) {
                throw new IllegalArgumentException("Invalid seek position: " + newPos);
            }
            this.pos = (int) newPos;
        }

        @Override
        public long skip(long n) {
            long k = Math.min(n, data.length - pos);
            pos += k;
            return k;
        }

        @Override
        public boolean markSupported() { return true; }

        @Override
        public void mark(int readLimit) { mark = pos; }

        @Override
        public void reset() { if (mark >= 0) pos = mark; }

        @Override
        public void close() { }
    }

    /* ============================================================
       Main processing logic
       ============================================================ */
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {

        final ComponentLog logger = getLogger();
        FlowFile input = session.get();
        if (input == null) return;

        FlowFile output = session.create(input);

        try {
            // Read full FlowFile into byte[]
            final ByteArrayOutputStream memory = new ByteArrayOutputStream();
            session.read(input, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    byte[] buf = new byte[8192];
                    int n;
                    while ((n = in.read(buf)) != -1) {
                        memory.write(buf, 0, n);
                    }
                }
            });

            final byte[] parquetBytes = memory.toByteArray();

            output = session.write(output, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {

                    InputFile inputFile = new InputFile() {
                        @Override public long getLength() { return parquetBytes.length; }
                        @Override public SeekableInputStream newStream() { return new ByteArraySeekableInputStream(parquetBytes); }
                    };

                    try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {

                        ParquetMetadata meta = reader.getFooter();
                        MessageType schema = meta.getFileMetaData().getSchema();

                        out.write("[".getBytes(StandardCharsets.UTF_8));
                        boolean first = true;

                        PageReadStore pages;
                        while ((pages = reader.readNextRowGroup()) != null) {
                            long rows = pages.getRowCount();

                            MessageColumnIO colIO = new ColumnIOFactory().getColumnIO(schema);
                            GroupRecordConverter conv = new GroupRecordConverter(schema);
                            RecordReader<Group> rr = colIO.getRecordReader(pages, conv);

                            for (int i = 0; i < rows; i++) {
                                Group g = rr.read();
                                if (!first) out.write(",".getBytes(StandardCharsets.UTF_8));
                                first = false;
                                writeGroupAsJson(g, schema, out);
                            }
                        }

                        out.write("]".getBytes(StandardCharsets.UTF_8));

                    } catch (Exception e) {
                        throw new IOException("Parquet read failed: " + e.getMessage(), e);
                    }
                }
            });

            output = session.putAttribute(output, "mime.type", "application/json");
            session.transfer(output, REL_SUCCESS);
            session.remove(input);

        } catch (Exception e) {

            try { session.remove(output); } catch (Exception ignore) {}

            session.transfer(input, REL_FAILURE);
            logger.error("Parquet conversion failed: {}", new Object[]{ e }, e);
        }
    }

    /* ============================================================
       Convert Parquet Group → JSON
       ============================================================ */
    private static void writeGroupAsJson(Group group, MessageType schema, OutputStream out) throws IOException {
        out.write('{');

        for (int i = 0; i < schema.getFieldCount(); i++) {
            Type field = schema.getType(i);
            String name = field.getName();

            if (i > 0) out.write(',');

            writeJsonString(name, out);
            out.write(':');

            int repCount = group.getFieldRepetitionCount(i);

            if (field.isRepetition(Type.Repetition.REPEATED)) {
                out.write('[');
                for (int r = 0; r < repCount; r++) {
                    writeValue(group, field, i, r, out);
                    if (r < repCount - 1) out.write(',');
                }
                out.write(']');

            } else {
                if (repCount == 0) {
                    out.write("null".getBytes(StandardCharsets.UTF_8));
                } else {
                    writeValue(group, field, i, 0, out);
                }
            }
        }

        out.write('}');
    }

    private static void writeValue(Group group, Type field, int fieldIndex, int repIndex, OutputStream out) throws IOException {
        if (field.isPrimitive()) {
            PrimitiveType.PrimitiveTypeName pt = field.asPrimitiveType().getPrimitiveTypeName();
            switch (pt) {
                case BINARY:
                case FIXED_LEN_BYTE_ARRAY:
                    writeJsonString(group.getBinary(fieldIndex, repIndex).toStringUsingUTF8(), out);
                    break;
                case INT32:
                    out.write(Integer.toString(group.getInteger(fieldIndex, repIndex)).getBytes(StandardCharsets.UTF_8));
                    break;
                case INT64:
                    out.write(Long.toString(group.getLong(fieldIndex, repIndex)).getBytes(StandardCharsets.UTF_8));
                    break;
                case FLOAT:
                    out.write(Float.toString(group.getFloat(fieldIndex, repIndex)).getBytes(StandardCharsets.UTF_8));
                    break;
                case DOUBLE:
                    out.write(Double.toString(group.getDouble(fieldIndex, repIndex)).getBytes(StandardCharsets.UTF_8));
                    break;
                case BOOLEAN:
                    out.write(Boolean.toString(group.getBoolean(fieldIndex, repIndex)).getBytes(StandardCharsets.UTF_8));
                    break;
                case INT96:
                    writeJsonString(group.getInt96(fieldIndex, repIndex).toString(), out);
                    break;
                default:
                    writeJsonString(group.getValueToString(fieldIndex, repIndex), out);
            }
        } else {
            Group nested = group.getGroup(fieldIndex, repIndex);
            MessageType nestedSchema = new MessageType("nested", field.asGroupType().getFields());
            writeGroupAsJson(nested, nestedSchema, out);
        }
    }

    private static void writeJsonString(String s, OutputStream out) throws IOException {
        out.write('"');
        final int len = s.length();
        for (int i = 0; i < len; i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': out.write("\\\"".getBytes(StandardCharsets.UTF_8)); break;
                case '\\': out.write("\\\\".getBytes(StandardCharsets.UTF_8)); break;
                case '\b': out.write("\\b".getBytes(StandardCharsets.UTF_8)); break;
                case '\f': out.write("\\f".getBytes(StandardCharsets.UTF_8)); break;
                case '\n': out.write("\\n".getBytes(StandardCharsets.UTF_8)); break;
                case '\r': out.write("\\r".getBytes(StandardCharsets.UTF_8)); break;
                case '\t': out.write("\\t".getBytes(StandardCharsets.UTF_8)); break;
                default:
                    if (c < 0x20) {
                        out.write(String.format("\\u%04x", (int) c).getBytes(StandardCharsets.UTF_8));
                    } else {
                        out.write((byte) c);
                    }
            }
        }
        out.write('"');
    }
}
