import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Created by alex on 31.5.19.
 */
@SuppressWarnings("DefaultFileTemplate")
public class Orgs {
    private String docID;
    private String CommonName;
    private String DocumentTitle;
    private String ParentOrganisationName;

    @JsonProperty("ParentOrganisationName")
    public String getParentOrganisationName() {
        return ParentOrganisationName;
    }

    public Orgs setParentOrganisationName(String parentOrganisationName) {
        ParentOrganisationName = parentOrganisationName;
        return this;
    }

    @JsonProperty("_id")
    public String getDocID() {
        return docID;
    }


    @JsonProperty("CommonName")
    public String getCommonName() {
        return CommonName;
    }


    public Orgs setCommonName(String commonName) {
        CommonName = commonName;
        return this;
    }


    @JsonProperty("DocumentTitle")
    public String getDocumentTitle() {
        return DocumentTitle;
    }

    public Orgs setDocumentTitle(String documentTitle) {
        DocumentTitle = documentTitle;
        return this;
    }

    public Orgs setDocID(String docID) {
        this.docID = docID;
        return this;
    }

    @Override
    public String toString() {
        return "Orgs{" +
                "docID='" + docID + '\'' +
                ", CommonName='" + CommonName + '\'' +
                ", DocumentTitle='" + DocumentTitle + '\'' +
                ", ParentOrganisationName='" + ParentOrganisationName + '\'' +
                '}';
    }
}
