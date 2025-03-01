package lambda;

public class Request {
    private String bucketname; // S3 bucket name
    private String filename;   // Name of the file in S3
    private String csvContent; 
    

    // Default constructor
    public Request() {}

    // Constructor with parameters
    public Request(String bucketname, String filename, String csvContent, String name) {
        this.bucketname = bucketname;
        this.filename = filename;
        this.csvContent = csvContent;
        
    }

    // Getter and Setter for bucketname
    public String getBucketname() {
        return bucketname;
    }

    public void setBucketname(String bucketname) {
        this.bucketname = bucketname;
    }

    // Getter and Setter for filename
    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    // Getter and Setter for csvContent
    public String getCsvContent() {
        return csvContent;
    }

    public void setCsvContent(String csvContent) {
        this.csvContent = csvContent;
    }

}

