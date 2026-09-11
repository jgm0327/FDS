package com.fdsv2.feedback;

import static org.assertj.core.api.Assertions.assertThat;

import com.fdsv2.decision.Action;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class FeedbackDatasetWriterTest {

    @TempDir
    private Path tempDir;

    private FeedbackDatasetRecord record(String decisionId) {
        return new FeedbackDatasetRecord(
                decisionId, "acc-1", Instant.now(), Action.BLOCK, 0.9, 0.9, "MODEL", 0.0,
                List.of("HARD_BLOCK"),
                List.of(new FeedbackTransactionStep(1.0, 300.0, false, "GROCERY")),
                1, "SIMULATED", 900, Instant.now());
    }

    @Test
    void 존재하지_않는_상위_디렉터리도_생성하고_한_줄로_append한다() throws IOException {
        Path outputPath = tempDir.resolve("nested/dir/dataset.jsonl");
        FeedbackDatasetWriter writer = new FeedbackDatasetWriter(outputPath.toString());

        writer.append(record("decision-1"));

        List<String> lines = Files.readAllLines(outputPath);
        assertThat(lines).hasSize(1);
        assertThat(lines.get(0)).contains("\"decisionId\":\"decision-1\"").contains("\"label\":1");
    }

    @Test
    void 여러_번_append하면_한_줄씩_누적된다() throws IOException {
        Path outputPath = tempDir.resolve("dataset.jsonl");
        FeedbackDatasetWriter writer = new FeedbackDatasetWriter(outputPath.toString());

        writer.append(record("decision-1"));
        writer.append(record("decision-2"));

        List<String> lines = Files.readAllLines(outputPath);
        assertThat(lines).hasSize(2);
        assertThat(lines.get(0)).contains("decision-1");
        assertThat(lines.get(1)).contains("decision-2");
    }
}
