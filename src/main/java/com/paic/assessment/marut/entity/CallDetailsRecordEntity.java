package com.paic.assessment.marut.entity;

import jakarta.persistence.*;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@Entity
@Table(name = "call_data_record")
public class CallDetailsRecordEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "ID", nullable = false, length = 150)
    private Integer id;

    @Column(name = "RECORD_DATE", nullable = false)
    private LocalDateTime recordDate;

    @Column(name = "L_SPC")
    private Integer lSpc;

    @Column(name = "L_SSN")
    private Integer lSsn;

    @Column(name = "L_RI")
    private Integer lRi;

    @Column(name = "L_GT_I")
    private Integer lGtI;

    @Column(name = "L_GT_DIGITS", length = 18)
    private String lGtDigits;

    @Column(name = "R_SPC")
    private Integer rSpc;

    @Column(name = "R_SSN")
    private Integer rSsn;

    @Column(name = "R_RI")
    private Integer rRi;

    @Column(name = "R_GT_I")
    private Integer rGtI;

    @Column(name = "R_GT_DIGITS", length = 18)
    private String rGtDigits;

    @Column(name = "SERVICE_CODE", length = 50)
    private String serviceCode;

    @Column(name = "OR_NATURE")
    private Integer orNature;

    @Column(name = "OR_PLAN")
    private Integer orPlan;

    @Column(name = "OR_DIGITS", length = 18)
    private String orDigits;

    @Column(name = "DE_NATURE")
    private Integer deNature;

    @Column(name = "DE_PLAN")
    private Integer dePlan;

    @Column(name = "DE_DIGITS", length = 18)
    private String deDigits;

    @Column(name = "ISDN_NATURE")
    private Integer isdnNature;

    @Column(name = "ISDN_PLAN")
    private Integer isdnPlan;

    @Column(name = "MSISDN", length = 18)
    private String msisdn;

    @Column(name = "VLR_NATURE")
    private Integer vlrNature;

    @Column(name = "VLR_PLAN")
    private Integer vlrPlan;

    @Column(name = "VLR_DIGITS", length = 18)
    private String vlrDigits;

    @Column(name = "IMSI", length = 100)
    private String imsi;

    @Column(name = "STATUS", nullable = false, length = 30)
    private String status;

    @Column(name = "TYPE", nullable = false, length = 30)
    private String type;

    @Column(name = "TSTAMP", nullable = false)
    private LocalDateTime tstamp;

    @Column(name = "LOCAL_DIALOG_ID")
    private Long localDialogId;

    @Column(name = "REMOTE_DIALOG_ID")
    private Long remoteDialogId;

    @Column(name = "DIALOG_DURATION")
    private Long dialogDuration;

    @Column(name = "USSD_STRING", length = 255)
    private String ussdString;


}
