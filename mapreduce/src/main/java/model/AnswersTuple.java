package model;

import com.opencsv.CSVParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Optional;

public class AnswersTuple implements Writable {
    private long id;
    private long ownerUserId;
    private Optional<ZonedDateTime> creationDate;
    private long parentId;
    private int score;
    private String body;

    public AnswersTuple(long id, long ownerUserId, String creationDate, int score, String body) {
        this.id = id;
        this.ownerUserId = ownerUserId;

        try {
            this.creationDate = Optional.of(ZonedDateTime.parse(creationDate));
        } catch (DateTimeParseException e) {
            e.printStackTrace();
            this.creationDate = Optional.empty();
        }

        this.score = score;
        this.body = body;
    }

    public AnswersTuple() {
        creationDate = Optional.empty();
    }

    public static AnswersTuple read(DataInput in) throws IOException {
        AnswersTuple tuple = new AnswersTuple();
        tuple.readFields(in);
        return tuple;
    }

    public static AnswersTuple parseCsvLine(Long key, Text line) throws IOException {
        // Skip if the input is csv header
        if (key == 0 && line.toString().contains("CreationDate")) {
            return null;
        }
        CSVParser parser = new CSVParser();
        AnswersTuple tuple = new AnswersTuple();
        String[] fields = parser.parseLine(line.toString());

        tuple.setId(Long.parseLong(fields[0]));
        tuple.setOwnerUserId(Long.parseLong(fields[1]));
        try {
            tuple.setCreationDate(Optional.of(ZonedDateTime.parse(fields[2])));
        } catch (DateTimeParseException e) {
            tuple.setCreationDate(Optional.empty());
        }
        tuple.setParentId(Long.parseLong(fields[3]));
        tuple.setScore(Integer.parseInt(fields[4]));
        tuple.setBody(fields[5]);
        return tuple;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(id);
        dataOutput.writeLong(ownerUserId);
        if (creationDate.isPresent()) {
            // Newline is needed to distinguish two serialized strings from each other
            dataOutput.writeBytes(creationDate.get() + "\n");
        } else {
            dataOutput.writeBytes("NULL\n");
        }
        dataOutput.writeLong(parentId);
        dataOutput.writeInt(score);
        dataOutput.writeBytes(body.replaceAll("(\r\n|\r|\n)", " ") + "\n");
    }

    @Override
    public String toString() {
        return "AnswersTuple{" +
                "id=" + id +
                ", ownerUserId=" + ownerUserId +
                ", creationDate=" + creationDate.orElse(null) +
                ", score=" + score +
                ", body='" + body + '\'' +
                '}';
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readLong();
        ownerUserId = dataInput.readLong();
        try {
            creationDate = Optional.of(ZonedDateTime.parse(dataInput.readLine().replace("\n", "")));
        } catch (DateTimeParseException e) {
            e.printStackTrace();
            this.creationDate = Optional.empty();
        }
        parentId = dataInput.readLong();
        score = dataInput.readInt();
        body = dataInput.readLine();
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getOwnerUserId() {
        return ownerUserId;
    }

    public void setOwnerUserId(long ownerUserId) {
        this.ownerUserId = ownerUserId;
    }

    public Optional<ZonedDateTime> getCreationDate() {
        return creationDate;
    }

    public void setCreationDate(Optional<ZonedDateTime> creationDate) {
        this.creationDate = creationDate;
    }

    public long getParentId() {
        return parentId;
    }

    public void setParentId(long parentId) {
        this.parentId = parentId;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }
}