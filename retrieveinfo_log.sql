USE [BankingETL]
GO

/****** Object:  Table [dbo].[retrieveinfo]    Script Date: 5/6/2025 8:02:57 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[retrieveinfo](
	[retrieve_id] [int] IDENTITY(1,1) NOT NULL,
	[source_file] [nvarchar](255) NULL,
	[retrieved_at] [datetime] NULL,
	[total_rows] [int] NULL,
	[processed_rows] [int] NULL,
	[errors] [int] NULL,
	[notes] [nvarchar](1000) NULL,
PRIMARY KEY CLUSTERED 
(
	[retrieve_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO


