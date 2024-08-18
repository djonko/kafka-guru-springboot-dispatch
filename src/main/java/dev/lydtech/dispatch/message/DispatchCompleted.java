package dev.lydtech.dispatch.message;

import lombok.*;

import java.util.UUID;

@NoArgsConstructor
@AllArgsConstructor
@Builder
@Data
@ToString
public class DispatchCompleted {
    private UUID orderId;
    private String date;
}