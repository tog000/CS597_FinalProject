cd('/home/tog/documents/escuela/2013/bigdata/final_project/reports/');

%% Import top 50.
filename = 'top50.csv';
delimiter = {','};
formatSpec = '%*s%*s%s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%*s%[^\n\r]';
fileID = fopen(filename,'r');
dataArray = textscan(fileID, formatSpec, 'Delimiter', delimiter,  'ReturnOnError', false);
fclose(fileID);
CRIME_NAMES = dataArray{:, 1};
clearvars filename delimiter formatSpec fileID dataArray ans;

%% Import most correlated.
filename = 'sorted_output.txt';
delimiter = {'\t'};
formatSpec = '%d%s%f%*[^\n\r]';
fileID = fopen(filename,'r');
dataArray = textscan(fileID, formatSpec, 'Delimiter', delimiter,  'ReturnOnError', false);
fclose(fileID);
TOP_CORRELATED_NAME = dataArray{:, 2};
TOP_CORRELATED = dataArray{:, 3};
clearvars filename delimiter formatSpec fileID dataArray ans;

%%

%Remove the first one, we know is e-8
TOP_CORRELATED_NAME(1) = [];
TOP_CORRELATED(1) = [];

total_correlations = 54;
folder = 'scatter';
%file_list=dir([folder '/scatter_E*']);
file_list = cell(total_correlations,1);
for i=1:total_correlations
    file_list{i} = ['/scatter_' TOP_CORRELATED_NAME{i} '-r-00000'];
end

figure_count = 1;

num_rows = 2;
num_cols = 3;

for n=1:length(file_list)
    if mod(n-1, num_rows*num_cols) == 0
        f = figure(100+figure_count);
        set(f,'Position', [100, 100, num_rows*600, num_cols*200])
        set(gcf,'PaperUnits','centimeters','PaperPosition',[0 0 50 30])
        figure_count = figure_count + 1;
    end
    subplot(num_rows,num_cols,mod(n-1, num_rows*num_cols)+1);
    name = file_list{n};
    data = load([folder '/' name]);
    indicator = name(11:length(name)-10);
    if indicator(end) == 'C'
        indicator = indicator(1:end-1);
    end
    indicator=strrep(indicator,'_',' ');
    name=strrep(name,'_',' ');
    
    
    crime_number = name(length(name)-9:end);
    if crime_number(1) == 'C'
        crime_number = crime_number(2);
    else
        crime_number = crime_number(1:2);
    end
    
    sprintf('Evaluating file %s.\nIndicator=%s\nCrime Number=%s\n\n',name, indicator, crime_number)
    
    crime_name=CRIME_NAMES{str2num(crime_number)};
    
    x = data(:,2);
    y = data(:,3);
    
    plot(x,y,'b.','MarkerSize',20);
    %axis([0 100 0 max(data(:,2))]);
    
    title(sprintf('File:%s\nSpearman Correlation = %G',name,spear(x,y)));
    
    ylabel(indicator)
    xlabel(crime_name)
   
    if mod(n-1, num_rows*num_cols) == (num_rows*num_cols)-1
        saveas(gcf,sprintf('figures/ScatterPlot%d.eps',round(n/(num_rows*num_cols))),'epsc');
    end
end