package lambda;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TransformTest {

    private static final Path FIXTURES = Path.of("..", "tests", "fixtures");

    @TempDir
    Path tempDir;

    @Test
    void transformFileMatchesSharedGoldenFile() throws Exception {
        Path output = tempDir.resolve("transformed.csv");

        Transform.TransformStats stats = Transform.transformFile(
                FIXTURES.resolve("sales_input.csv").toString(),
                output.toString()
        );

        assertEquals(3, stats.rowsWritten());
        assertEquals(1, stats.duplicateRows());
        assertEquals(
                normalizedLines(FIXTURES.resolve("expected").resolve("transformed_sales.csv")),
                normalizedLines(output)
        );
    }

    private static List<String> normalizedLines(Path path) throws Exception {
        return Files.readAllLines(path).stream()
                .map(line -> line.replace("\r", ""))
                .toList();
    }
}
