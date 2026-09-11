package com.fdsv2.feedback;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * CP6 조인 결과({@link FeedbackDatasetRecord})를 JSONL 파일에 한 줄씩 append한다.
 *
 * <p><b>왜 새 Kafka 토픽이 아니라 파일인가</b> — AI 쪽({@code ai/})이 Python Kafka 클라이언트
 * 의존성을 새로 추가하지 않아도 되고, {@code ai/artifacts}처럼 "언제든 재생성 가능한 로컬 산출물"로
 * 취급하기 자연스럽다(ai/README.md "학습 산출물을 커밋하지 않는 이유" 참고). 대신
 * {@code .gitignore}에 포함해 커밋하지 않는다.
 *
 * <p><b>알려진 한계</b>: 이 프로젝트는 단일 인스턴스 전제라 파일 append에 프로세스 간 락이 없다 —
 * 여러 인스턴스가 동시에 쓰면 줄이 섞일 수 있다(JVM 내부에서는 {@code synchronized}로 막는다).
 * 운영 규모라면 공유 스토리지나 Kafka 토픽으로 바꿔야 한다. 쓰기 자체가 실패하면(디스크 풀 등)
 * 예외를 던지고, 호출부({@link FeedbackLabelScheduler})가 이를 "이 레코드는 버린다"로 처리한다 —
 * 재시도/DLQ는 이번 범위 밖.
 */
@Component
public class FeedbackDatasetWriter {

    private final Path outputPath;
    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public FeedbackDatasetWriter(
            @Value("${fds.feedback.dataset-output-path:data/feedback/labeled-dataset.jsonl}") String outputPath) {
        this.outputPath = Path.of(outputPath);
    }

    public synchronized void append(FeedbackDatasetRecord record) {
        try {
            Path parent = outputPath.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            String line = objectMapper.writeValueAsString(record);
            Files.writeString(outputPath, line + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("CP6 피드백 데이터셋 파일 쓰기 실패: " + outputPath, e);
        }
    }
}
