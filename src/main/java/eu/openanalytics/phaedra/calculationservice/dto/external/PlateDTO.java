package eu.openanalytics.phaedra.calculationservice.dto.external;

import eu.openanalytics.phaedra.calculationservice.enumeration.external.ApprovalStatus;
import eu.openanalytics.phaedra.calculationservice.enumeration.external.CalculationStatus;
import eu.openanalytics.phaedra.calculationservice.enumeration.external.LinkStatus;
import eu.openanalytics.phaedra.calculationservice.enumeration.external.UploadStatus;
import eu.openanalytics.phaedra.calculationservice.enumeration.external.ValidationStatus;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@NoArgsConstructor
public class PlateDTO {

	private Long id;
	private String barcode;
	private String description;
	private Long experimentId;

	private Integer rows;
	private Integer columns;
	private Integer sequence;
	
	private LinkStatus linkStatus;
	private String linkSource;
	private String linkTemplateId;
	private Date linkedOn;

	private CalculationStatus calculationStatus;
	private String calculationError;
	private String calculatedBy;
	private Date calculatedOn;

	private ValidationStatus validationStatus;
	private String validatedBy;
	private Date validatedOn;

	private ApprovalStatus approvalStatus;
	private String approvedBy;
	private Date approvedOn;

	private UploadStatus uploadStatus;
	private String uploadedBy;
	private Date uploadedOn;

	private Date createdOn;
	private String createdBy;
	private Date updatedOn;
	private String updatedBy;
}
