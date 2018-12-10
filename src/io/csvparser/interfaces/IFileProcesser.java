package io.csvparser.interfaces;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

public interface IFileProcesser {
     void perform(List<Path> paths) throws IOException;
}
