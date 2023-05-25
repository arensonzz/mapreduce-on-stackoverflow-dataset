package model;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

public class QuestionsTuple implements Writable {
    private long id;
    private long ownerUserId;
    private Optional<ZonedDateTime> creationDate;
    private int score;
    private String title;
    private String body;
    private ArrayList<String> tags;

    public QuestionsTuple(long id, long ownerUserId, String creationDate, String closedDate, int score, String title, String body, String tags) {
        this.id = id;
        this.ownerUserId = ownerUserId;

        try {
            this.creationDate = Optional.of(ZonedDateTime.parse(creationDate));
        } catch (DateTimeParseException e) {
            e.printStackTrace();
            this.creationDate = Optional.empty();
        }

        this.score = score;
        this.title = title;
        this.body = body;
        this.setTags(tags);
    }

    public QuestionsTuple() {
        creationDate = Optional.empty();
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
        dataOutput.writeInt(score);
        dataOutput.writeBytes(title + "\n");
        dataOutput.writeBytes(body + "\n");
        dataOutput.writeBytes(String.join(" ", tags) + "\n");
    }

    @Override
    public String toString() {
        return "QuestionsTuple{" +
                "id=" + id +
                ", ownerUserId=" + ownerUserId +
                ", creationDate=" + creationDate.orElse(null) +
                ", score=" + score +
                ", title='" + title + '\'' +
                ", body='" + body + '\'' +
                ", tags='" + tags + '\'' +
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
        score = dataInput.readInt();
        title = dataInput.readLine();
        body = dataInput.readLine();
        setTags(dataInput.readLine());
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

    public ArrayList<String> getTags() {
        return tags;
    }

    public void setTags(String tags) {
        this.tags = new ArrayList<>();
        this.tags.addAll(Arrays.asList(tags.trim().split("\\s+")));
    }
}
