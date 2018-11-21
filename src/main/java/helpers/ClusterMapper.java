package helpers;

public interface ClusterMapper {
    default long map(long vertexId){
        return vertexId%3;
    }
}
