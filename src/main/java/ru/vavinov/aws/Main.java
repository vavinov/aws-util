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
    private static final int PART_SIZE = 5 * 1048576;

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
        private String expectedEtag;

        @JsonCreator
        public Session(
                @JsonProperty("file") File file,
                @JsonProperty("target") S3Target target,
                @JsonProperty("uploadId") String uploadId,
                @JsonProperty("parts") List<Part> parts,
                @JsonProperty("expectedEtag") String expectedEtag)
        {
            this.file = file;
            this.target = target;
            this.uploadId = uploadId;
            this.parts = parts;
            this.expectedEtag = expectedEtag;
        }
    }

    public static List<PartRange> toRanges(long n, int maxRangeLength) {
        List<PartRange> result = Lists.newArrayList();
        long offset = 0;
        for (int number = 1; offset < n; ++ number) {
            int partLength = Math.min(maxRangeLength, (int) (n - offset));
            result.add(new PartRange(number, offset, partLength));
            offset += partLength;
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
                .map((etag) -> BaseEncoding.base16().decode(etag))
                .collect(Collectors.<byte[]>toList())
                .toArray(new byte[0][]));
        return BaseEncoding.base16().encode(Md5Utils.computeMD5Hash(concatenatedEtags)) + "-" + partEtags.size();
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

            final String uploadId = client.initiateMultipartUpload(
                    new InitiateMultipartUploadRequest(target.bucket, target.key)).getUploadId();
            System.out.println("Got uploadId=" + uploadId);

            session = new Session(dataFile, target, uploadId, toRanges(dataFile.length(), PART_SIZE).stream()
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
            System.out.println(" etag=" + etag.getETag());
            part.etag = etag.getETag();

            saveSession(session, sessionStatusFile);
        }

        if (!session.parts.stream().allMatch((part) -> part.etag != null)) {
            System.err.println("Some parts are still not uploaded :(");
            System.exit(1);
        }

        if (session.expectedEtag == null) {
            session.expectedEtag = s3multipartEtag(session.parts.stream()
                    .map((part) -> part.etag)
                    .collect(Collectors.<String>toList()));
        }

        System.out.println("Expected ETag: " + session.expectedEtag);

        CompleteMultipartUploadResult complete = client.completeMultipartUpload(new CompleteMultipartUploadRequest(
                session.target.bucket, session.target.key, session.uploadId,
                session.parts.stream()
                        .map((part) -> new PartETag(part.range.number, part.etag))
                        .collect(Collectors.<PartETag>toList())));

        System.out.println("Location: " + complete.getLocation());
    }
}
