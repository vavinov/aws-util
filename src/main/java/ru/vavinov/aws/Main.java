package ru.vavinov.aws;

import java.io.File;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.ProgressEvent;
import com.amazonaws.services.s3.model.ProgressListener;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.util.Md5Utils;
import com.google.common.collect.Lists;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Bytes;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;

// TODO use Optional instead of nulls

public class Main {
    // http://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
    private static final long MIN_PART_SIZE = 5L * 1024 * 1024;
    private static final long MAX_PART_SIZE = 5L * 1024 * 1024 * 1024;
    private static final long MAX_TOTAL_SIZE = 5L * 1024 * 1024 * 1024 * 1024;
    private static final long MAX_PART_COUNT = 10000L;

    public static class PartRange {
        @JsonProperty
        private final int number;
        @JsonProperty
        private final long offset;
        @JsonProperty
        private final int length;

        @JsonCreator
        public PartRange(
                @JsonProperty("number") int number,
                @JsonProperty("offset") long offset,
                @JsonProperty("length") int length)
        {
            this.number = number;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public String toString() {
            return "[#" + number + " offset=" + offset + " size=" + length + "]";
        }
    }

    public static class Part {
        @JsonProperty
        private final PartRange range;
        @JsonProperty
        @Nullable
        private String etag;

        @JsonCreator
        public Part(
                @JsonProperty("range") PartRange range,
                @JsonProperty("etag") String etag)
        {
            this.range = range;
            this.etag = etag;
        }
    }

    public static class S3Target {
        @JsonProperty
        private final String bucket;
        @JsonProperty
        private final String key;

        @JsonCreator
        public S3Target(
                @JsonProperty("bucket") String bucket,
                @JsonProperty("key") String key)
        {
            this.bucket = bucket;
            this.key = key;
        }
    }

    public static class Session {
        @JsonProperty
        private final File file;
        @JsonProperty
        private final S3Target target;
        @JsonProperty
        private final String uploadId;
        @JsonProperty
        private final List<Part> parts;
        @JsonProperty
        @Nullable
        private String multipartEtag;

        @JsonCreator
        public Session(
                @JsonProperty("file") File file,
                @JsonProperty("target") S3Target target,
                @JsonProperty("uploadId") String uploadId,
                @JsonProperty("parts") List<Part> parts,
                @JsonProperty("multipartEtag") String multipartEtag)
        {
            this.file = file;
            this.target = target;
            this.uploadId = uploadId;
            this.parts = parts;
            this.multipartEtag = multipartEtag;
        }
    }

    private static int toInt(long l) {
        if (l < Integer.MIN_VALUE || l > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("long does not fit into int");
        }
        return (int) l;
    }

    public static List<PartRange> toRanges(long n, int maxRangeLength) {
        int capacity = toInt(n / maxRangeLength) + 1;
        List<PartRange> result = Lists.newArrayListWithCapacity(capacity);
        //System.err.println("Expected # of parts: " + capacity);
        long offset = 0;
        for (int number = 1; offset < n; ++ number) {
            int partLength = toInt(Math.min((long) maxRangeLength, n - offset));
            result.add(new PartRange(number, offset, partLength));
            offset += partLength;
            //System.err.println(": " + number + " o " + offset + " pl " + partLength + " n=" + n);
        }
        return result;
    }

    private static Exception usageAndExit() {
        System.err.println("usage:");
        System.err.println("\t" + Main.class.getName() + " --upload <bucket> <key> <data-file> <status-file>");
        System.err.println("\t" + Main.class.getName() + " --resume-upload <status-file>");
        System.exit(1);
        throw null;
    }

    private static void saveSession(Session session, File file) throws IOException {
        new ObjectMapper().defaultPrettyPrintingWriter().writeValue(file, session);
    }

    private static Session loadSession(File file) throws IOException {
        return new ObjectMapper().readValue(file, Session.class);
    }

    // unofficial spec:
    // http://permalink.gmane.org/gmane.comp.file-systems.s3.s3tools/583
    private static String s3multipartEtag(List<String> partEtags) throws IOException, NoSuchAlgorithmException {
        byte[] concatenatedEtags = Bytes.concat(partEtags.stream()
                .map((etag) -> BaseEncoding.base16().decode(etag.toUpperCase()))
                .collect(Collectors.<byte[]>toList())
                .toArray(new byte[0][]));
        return BaseEncoding.base16().encode(Md5Utils.computeMD5Hash(concatenatedEtags)).toLowerCase()
                + "-" + partEtags.size();
    }

    private static int minimalPartSize(long totalSize) {
        if (totalSize > MAX_TOTAL_SIZE) {
            throw new IllegalArgumentException();
        }
        return toInt(Math.max(MIN_PART_SIZE, totalSize / MAX_PART_COUNT));
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            throw usageAndExit();
        }

        final AmazonS3Client client = new AmazonS3Client(new SystemPropertiesCredentialsProvider());

        String action = args[0];

        final Session session;
        final File sessionStatusFile;

        if ("--upload".equals(action)) {
            final S3Target target = new S3Target(args[1], args[2]);
            final File dataFile = new File(args[3]);

            sessionStatusFile = new File(args[4]);

            long dataFileSize = dataFile.length();
            int partSize = minimalPartSize(dataFileSize);
            System.out.println("Data file size=" + dataFileSize + ", will use part size=" + partSize);

            final String uploadId = client.initiateMultipartUpload(
                    new InitiateMultipartUploadRequest(target.bucket, target.key)).getUploadId();
            System.out.println("Got uploadId=" + uploadId);


            session = new Session(dataFile, target, uploadId, toRanges(dataFile.length(), partSize).stream()
                    .map((partRange) -> new Part(partRange, null))
                    .collect(Collectors.<Part>toList()), null);

            saveSession(session, sessionStatusFile);

        } else if ("--resume-upload".equals(action)) {
            sessionStatusFile = new File(args[1]);
            session = loadSession(sessionStatusFile);

        } else {
            throw usageAndExit();
        }

        List<Part> partsToUpload = session.parts.stream()
                .filter((part) -> part.etag == null).
                collect(Collectors.<Part>toList());

        System.out.println("Total parts=" + session.parts.size() + ", need to upload=" + partsToUpload.size());

        for (Part part : partsToUpload) {
            System.out.print("Uploading part #" + part.range.number);
            System.out.flush();
            PartETag etag = client.uploadPart(new UploadPartRequest()
                    .withUploadId(session.uploadId)
                    .withBucketName(session.target.bucket)
                    .withKey(session.target.key)
                    .withPartNumber(part.range.number)
                    .withPartSize(part.range.length)
                    .withFile(session.file)
                    .withFileOffset(part.range.offset)
                    .withProgressListener(new ProgressListener() {
                        private static final int PERIOD = 1048576;
                        private long transferred = 0;

                        @Override
                        public void progressChanged(ProgressEvent progressEvent) {
                            transferred += progressEvent.getBytesTransfered();
                            if (transferred > PERIOD) {
                                transferred %= PERIOD;
                                System.out.print(".");
                                System.out.flush();
                            }
                        }
                    })
            ).getPartETag();
            System.out.println(" ETag=" + etag.getETag());
            part.etag = etag.getETag();

            saveSession(session, sessionStatusFile);
        }

        if (!session.parts.stream().allMatch((part) -> part.etag != null)) {
            System.err.println("Some parts are still not uploaded :(");
            System.exit(1);
        } else {
            System.out.println("All parts are uploaded, completing multipart upload...");
        }

        if (session.multipartEtag == null) {
            session.multipartEtag = s3multipartEtag(session.parts.stream()
                    .map((part) -> part.etag)
                    .collect(Collectors.<String>toList()));
        }

        System.out.println("Multipart ETag: " + session.multipartEtag);

        CompleteMultipartUploadResult complete = client.completeMultipartUpload(new CompleteMultipartUploadRequest(
                session.target.bucket, session.target.key, session.uploadId,
                Lists.newArrayList(session.parts.stream()
                        .map((part) -> new PartETag(part.range.number, part.etag))
                        .iterator())));

        System.out.println("Location: " + complete.getLocation());
    }
}
