package model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Optional;

public class QuestionsTuple implements Writable {
    private long id;
    private long ownerUserId;
    private Optional<ZonedDateTime> creationDate;
    private Optional<ZonedDateTime> closedDate;
    private int score;
    private String title;
    private String body;

    public QuestionsTuple(long id, long ownerUserId, String creationDate, String closedDate, int score, String title, String body) {
        this.id = id;
        this.ownerUserId = ownerUserId;

        try {
            this.creationDate = Optional.of(ZonedDateTime.parse(creationDate));
        } catch (DateTimeParseException e) {
            e.printStackTrace();
            this.creationDate = Optional.empty();
        }
        try {
            this.closedDate = Optional.of(ZonedDateTime.parse(closedDate));
        } catch (DateTimeParseException e) {
            e.printStackTrace();
            this.closedDate = Optional.empty();
        }

        this.score = score;
        this.title = title;
        this.body = body;
    }

    public QuestionsTuple() {
        creationDate = Optional.empty();
        closedDate = Optional.empty();
    }

    public static QuestionsTuple read(DataInput in) throws IOException {
        QuestionsTuple tuple = new QuestionsTuple();
        tuple.readFields(in);
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
        if (closedDate.isPresent()) {
            dataOutput.writeBytes(closedDate.get() + "\n");
        } else {
            dataOutput.writeBytes("NULL\n");
        }
        dataOutput.writeInt(score);
        dataOutput.writeBytes(title.replaceAll("(\r\n|\r|\n)", " ") + "\n");
        dataOutput.writeBytes(body.replaceAll("(\r\n|\r|\n)", " ") + "\n");
    }

    @Override
    public String toString() {
        return "QuestionsTuple{" +
                "id=" + id +
                ", ownerUserId=" + ownerUserId +
                ", creationDate=" + creationDate.orElse(null) +
                ", closedDate=" + closedDate.orElse(null) +
                ", score=" + score +
                ", title='" + title + '\'' +
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
        try {
            closedDate = Optional.of(ZonedDateTime.parse(dataInput.readLine().replace("\n", "")));
        } catch (DateTimeParseException e) {
            e.printStackTrace();
            this.closedDate = Optional.empty();
        }
        score = dataInput.readInt();
        title = dataInput.readLine();
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

    public Optional<ZonedDateTime> getClosedDate() {
        return closedDate;
    }

    public void setClosedDate(Optional<ZonedDateTime> closedDate) {
        this.closedDate = closedDate;
    }

    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }
}
