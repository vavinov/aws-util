package ru.vavinov.aws;

import java.io.File;
import java.util.List;

import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadResult;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.ProgressEvent;
import com.amazonaws.services.s3.model.ProgressListener;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.google.common.collect.Lists;

public class Main {
    private static final int PART_SIZE = 10 * 1048576;

    public static class PartRange {
        private final int number;
        private final long offset;
        private final int length;

        public PartRange(int number, long offset, int length) {
            this.number = number;
            this.offset = offset;
            this.length = length;
        }

        @Override
        public String toString() {
            return "[#" + number + " offset=" + offset + " size=" + length + "]";
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

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("usage:  " + Main.class.getName() + " <bucket> <key> <file> [<uploadid>] [<start-part-n>]");
            System.exit(1);
        }

        final String bucket = args[0];
        final String key = args[1];
        final File file = new File(args[2]);

        final AmazonS3Client client = new AmazonS3Client(new SystemPropertiesCredentialsProvider());

        final String uploadId = (args.length > 3) ? args[3]
                : client.initiateMultipartUpload(new InitiateMultipartUploadRequest(bucket, key)).getUploadId();

        final int firstPartNumber = (args.length > 4) ? Integer.parseInt(args[4]) : 1;

        System.out.println("Got uploadId=" + uploadId);

        List<PartRange> partRanges = toRanges(file.length(), PART_SIZE);
        System.out.println("Total parts=" + partRanges.size() + ", starting from " + firstPartNumber);

        List<PartETag> etags = Lists.newArrayList();
        for (PartRange range : partRanges.subList(firstPartNumber - 1, partRanges.size())) {
            if (range.number < firstPartNumber) {
                continue; // XXX
            }

            System.out.print("Uploading part #" + range.number);
            System.out.flush();
            PartETag etag = client.uploadPart(new UploadPartRequest()
                    .withUploadId(uploadId)
                    .withBucketName(bucket)
                    .withKey(key)
                    .withPartNumber(range.number)
                    .withPartSize(range.length)
                    .withFile(file)
                    .withFileOffset(range.offset)
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
            etags.add(etag);
        }

        CompleteMultipartUploadResult complete = client.completeMultipartUpload(
                new CompleteMultipartUploadRequest(bucket, key, uploadId, etags));

        System.out.println(complete.getLocation());
    }
}
