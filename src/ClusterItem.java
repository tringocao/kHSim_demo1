public class ClusterItem implements Comparable<ClusterItem> {

    private int clusterId;
    private String url;
    private int nos;
    private String sh;
    private int similarityScore;

    public ClusterItem() {
    }

    public int getClusterId() {
        return clusterId;
    }

    public void setClusterId(int clusterId) {
        this.clusterId = clusterId;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getNos() {
        return nos;
    }

    public void setNos(int nos) {
        this.nos = nos;
    }

    public String getSh() {
        return sh;
    }

    public void setSh(String sh) {
        this.sh = sh;
    }

    public int getSimilarityScore() {
        return similarityScore;
    }

    public void setSimilarityScore(int similarityScore) {
        this.similarityScore = similarityScore;
    }

    @Override
    public int compareTo(ClusterItem o) {
        if(this.getSimilarityScore() > o.getSimilarityScore()) {
            return 1;
        } else if(this.getSimilarityScore() == o.getSimilarityScore()) {
            return 0;
        } else {
            return -1;
        }
    }

}